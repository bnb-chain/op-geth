package core

import (
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/metrics"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const (
	parallelPrimarySlot = 0
	parallelShadowSlot  = 1
	stage2CheckNumber   = 30 // ConfirmStage2 will check this number of transaction, to avoid too busy stage2 check
	stage2AheadNum      = 3  // enter ConfirmStage2 in advance to avoid waiting for Fat Tx
)

var (
	FallbackToSerialProcessorErr = errors.New("fallback to serial processor")
)

type ResultHandleEnv struct {
	statedb *state.StateDB
	gp      *GasPool
	txCount int
}

type ParallelStateProcessor struct {
	StateProcessor
	parallelNum           int          // leave a CPU to dispatcher
	slotState             []*SlotState // idle, or pending messages
	allTxReqs             []*ParallelTxRequest
	txResultChan          chan *ParallelTxResult // to notify dispatcher that a tx is done
	mergedTxIndex         atomic.Int32           // the latest finalized tx index
	pendingConfirmResults *sync.Map              // tx could be executed several times, with several result to check
	unconfirmedResults    *sync.Map              // for stage2 confirm, since pendingConfirmResults can not be accessed in stage2 loop
	unconfirmedDBs        *sync.Map              // intermediate store of slotDB that is not verified
	slotDBsToRelease      []*state.ParallelStateDB
	stopSlotChan          chan struct{}
	stopConfirmChan       chan struct{}
	debugConflictRedoNum  int

	confirmStage2Chan     chan int
	stopConfirmStage2Chan chan struct{}
	txReqExecuteRecord    map[int]int
	txReqExecuteCount     int
	inConfirmStage2       bool
	targetStage2Count     int
	nextStage2TxIndex     int
	delayGasFee           bool

	commonTxs         []*types.Transaction
	receipts          types.Receipts
	error             error
	resultMutex       sync.RWMutex
	resultProcessChan chan *ResultHandleEnv
	resultAppendChan  chan struct{}
}

func newParallelStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine, parallelNum int) *ParallelStateProcessor {
	processor := &ParallelStateProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		parallelNum:    parallelNum,
	}
	processor.init()
	return processor
}

type MergedTxInfo struct {
	slotDB              *state.StateDB
	StateObjectSuicided map[common.Address]struct{}
	StateChangeSet      map[common.Address]state.StateKeys
	BalanceChangeSet    map[common.Address]struct{}
	CodeChangeSet       map[common.Address]struct{}
	AddrStateChangeSet  map[common.Address]struct{}
	txIndex             int
}

type SlotState struct {
	pendingTxReqList  []*ParallelTxRequest
	primaryWakeUpChan chan struct{}
	shadowWakeUpChan  chan struct{}
	primaryStopChan   chan struct{}
	shadowStopChan    chan struct{}
	activatedType     int32 // 0: primary slot, 1: shadow slot
}

type ParallelTxResult struct {
	executedIndex int32 // record the current execute number of the tx
	slotIndex     int
	txReq         *ParallelTxRequest
	receipt       *types.Receipt
	slotDB        *state.ParallelStateDB
	gpSlot        *GasPool
	evm           *vm.EVM
	result        *ExecutionResult
	originalNonce *uint64
	err           error
}

type ParallelTxRequest struct {
	txIndex         int
	baseStateDB     *state.StateDB
	staticSlotIndex int
	tx              *types.Transaction
	gasLimit        uint64
	msg             *Message
	block           *types.Block
	vmConfig        vm.Config
	usedGas         *uint64
	curTxChan       chan int
	runnable        int32 // 0: not runnable 1: runnable - can be scheduled
	executedNum     atomic.Int32
	conflictIndex   atomic.Int32 // the conflicted mainDB index, the txs will not be executed before this number
	useDAG          bool
}

// init to initialize and start the execution goroutines
func (p *ParallelStateProcessor) init() {
	log.Info("Parallel execution mode is enabled", "Parallel Num", p.parallelNum,
		"CPUNum", runtime.NumCPU())
	p.txResultChan = make(chan *ParallelTxResult, 20000)
	p.stopSlotChan = make(chan struct{}, 1)
	p.stopConfirmChan = make(chan struct{}, 1)
	p.stopConfirmStage2Chan = make(chan struct{}, 1)

	p.resultProcessChan = make(chan *ResultHandleEnv, 1)
	p.resultAppendChan = make(chan struct{}, 20000)

	p.slotState = make([]*SlotState, p.parallelNum)
	quickMergeNum := 2 // p.parallelNum / 2
	for i := 0; i < p.parallelNum-quickMergeNum; i++ {
		p.slotState[i] = &SlotState{
			primaryWakeUpChan: make(chan struct{}, 1),
			shadowWakeUpChan:  make(chan struct{}, 1),
			primaryStopChan:   make(chan struct{}, 1),
			shadowStopChan:    make(chan struct{}, 1),
		}
		// start the primary slot's goroutine
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, parallelPrimarySlot) // this loop will be permanent live
		}(i)

		// start the shadow slot.
		// It is back up of the primary slot to make sure transaction can be redone ASAP,
		// since the primary slot could be busy at executing another transaction
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, parallelShadowSlot)
		}(i)
	}

	for i := p.parallelNum - quickMergeNum; i < p.parallelNum; i++ {
		// init a quick merge slot
		p.slotState[i] = &SlotState{
			primaryWakeUpChan: make(chan struct{}, 1),
			shadowWakeUpChan:  make(chan struct{}, 1),
			primaryStopChan:   make(chan struct{}, 1),
			shadowStopChan:    make(chan struct{}, 1),
		}
		go func(slotIndex int) {
			p.runQuickMergeSlotLoop(slotIndex, parallelPrimarySlot)
		}(i)
		go func(slotIndex int) {
			p.runQuickMergeSlotLoop(slotIndex, parallelShadowSlot)
		}(i)
	}

	p.confirmStage2Chan = make(chan int, 10)
	go func() {
		p.runConfirmStage2Loop()
	}()

	go func() {
		p.handlePendingResultLoop()
	}()
}

// resetState clear slot state for each block.
func (p *ParallelStateProcessor) resetState(txNum int, statedb *state.StateDB) {
	if txNum == 0 {
		return
	}
	p.mergedTxIndex.Store(-1)
	p.debugConflictRedoNum = 0
	p.inConfirmStage2 = false

	statedb.PrepareForParallel()
	p.allTxReqs = make([]*ParallelTxRequest, 0, txNum)

	for _, slot := range p.slotState {
		slot.pendingTxReqList = make([]*ParallelTxRequest, 0)
		slot.activatedType = parallelPrimarySlot
	}
	p.unconfirmedResults = new(sync.Map)
	p.unconfirmedDBs = new(sync.Map)
	p.pendingConfirmResults = new(sync.Map)
	p.txReqExecuteRecord = make(map[int]int, txNum)
	p.txReqExecuteCount = 0
	p.nextStage2TxIndex = 0
}

// Benefits of StaticDispatch:
//
//	** try best to make Txs with same From() in same slot
//	** reduce IPC cost by dispatch in Unit
//	** make sure same From in same slot
//	** try to make it balanced, queue to the most hungry slot for new Address
func (p *ParallelStateProcessor) doStaticDispatch(txReqs []*ParallelTxRequest) {
	fromSlotMap := make(map[common.Address]int, 100)
	toSlotMap := make(map[common.Address]int, 100)
	for _, txReq := range txReqs {
		var slotIndex = -1
		if i, ok := fromSlotMap[txReq.msg.From]; ok {
			// first: same From goes to same slot
			slotIndex = i
		} else if txReq.msg.To != nil {
			// To Address, with txIndex sorted, could be in different slot.
			if i, ok := toSlotMap[*txReq.msg.To]; ok {
				slotIndex = i
			}
		}

		// not found, dispatch to most hungry slot
		if slotIndex == -1 {
			slotIndex = p.mostHungrySlot()
		}
		// update
		fromSlotMap[txReq.msg.From] = slotIndex
		if txReq.msg.To != nil {
			toSlotMap[*txReq.msg.To] = slotIndex
		}

		slot := p.slotState[slotIndex]
		txReq.staticSlotIndex = slotIndex // txReq is better to be executed in this slot
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
	}
}

func (p *ParallelStateProcessor) mostHungrySlot() int {
	var (
		workload  = len(p.slotState[0].pendingTxReqList)
		slotIndex = 0
	)
	for i, slot := range p.slotState { // can start from index 1
		if len(slot.pendingTxReqList) < workload {
			slotIndex = i
			workload = len(slot.pendingTxReqList)
		}
		// just return the first slot with 0 workload
		if workload == 0 {
			return slotIndex
		}
	}
	return slotIndex
}

// hasConflict conducts conflict check
func (p *ParallelStateProcessor) hasConflict(txResult *ParallelTxResult, isStage2 bool) bool {
	slotDB := txResult.slotDB
	if txResult.err != nil {
		log.Info("HasConflict due to err", "err", txResult.err)
		return true
	} else if slotDB.NeedsRedo() {
		log.Info("HasConflict needsRedo")
		// if there is any reason that indicates this transaction needs to redo, skip the conflict check
		return true
	} else {
		// check whether the slot db reads during execution are correct.
		if !slotDB.IsParallelReadsValid(isStage2) {
			return true
		}
	}
	return false
}

func (p *ParallelStateProcessor) switchSlot(slotIndex int) {
	slot := p.slotState[slotIndex]
	if atomic.CompareAndSwapInt32(&slot.activatedType, parallelPrimarySlot, parallelShadowSlot) {
		// switch from normal to shadow slot
		if len(slot.shadowWakeUpChan) == 0 {
			slot.shadowWakeUpChan <- struct{}{}
		}
	} else if atomic.CompareAndSwapInt32(&slot.activatedType, parallelShadowSlot, parallelPrimarySlot) {
		// switch from shadow to normal slot
		if len(slot.primaryWakeUpChan) == 0 {
			slot.primaryWakeUpChan <- struct{}{}
		}
	}
}

// executeInSlot do tx execution with thread local slot.
func (p *ParallelStateProcessor) executeInSlot(slotIndex int, txReq *ParallelTxRequest) *ParallelTxResult {
	mIndex := p.mergedTxIndex.Load()
	conflictIndex := txReq.conflictIndex.Load()
	if mIndex < conflictIndex {
		// The conflicted TX has not been finished executing, skip.
		// the transaction failed at check(nonce or balance), actually it has not been executed yet.
		atomic.CompareAndSwapInt32(&txReq.runnable, 0, 1)
		return nil
	}
	execNum := txReq.executedNum.Add(1)
	slotDB := state.NewSlotDB(txReq.baseStateDB, txReq.txIndex, int(mIndex), p.unconfirmedDBs, txReq.useDAG)
	blockContext := NewEVMBlockContext(txReq.block.Header(), p.bc, nil, p.config, slotDB) // can share blockContext within a block for efficiency
	txContext := NewEVMTxContext(txReq.msg)
	vmenv := vm.NewEVM(blockContext, txContext, slotDB, p.config, txReq.vmConfig)

	rules := p.config.Rules(txReq.block.Number(), blockContext.Random != nil, blockContext.Time)
	slotDB.Prepare(rules, txReq.msg.From, vmenv.Context.Coinbase, txReq.msg.To, vm.ActivePrecompiles(rules), txReq.msg.AccessList)

	// gasLimit not accurate, but it is ok for block import.
	// each slot would use its own gas pool, and will do gas limit check later
	gpSlot := new(GasPool).AddGas(txReq.gasLimit) // block.GasLimit()

	on := txReq.tx.Nonce()
	if txReq.msg.IsDepositTx && p.config.IsOptimismRegolith(vmenv.Context.Time) {
		on = slotDB.GetNonce(txReq.msg.From)
	}

	slotDB.SetTxContext(txReq.tx.Hash(), txReq.txIndex)
	evm, result, err := applyTransactionStageExecution(txReq.msg, gpSlot, slotDB, vmenv, p.delayGasFee)
	txResult := ParallelTxResult{
		executedIndex: execNum,
		slotIndex:     slotIndex,
		txReq:         txReq,
		receipt:       nil,
		slotDB:        slotDB,
		err:           err,
		gpSlot:        gpSlot,
		evm:           evm,
		result:        result,
		originalNonce: &on,
	}

	if err == nil {
		p.unconfirmedDBs.Store(txReq.txIndex, slotDB)
	} else {
		// the transaction failed at check(nonce or balance), actually it has not been executed yet.
		// the error here can be either expected or unexpected.
		// expected - the execution is correct and the error is normal result
		// unexpected -  the execution is incorrectly accessed the state because of parallelization.
		// In both case, rerun with next version of stateDB, it is a waste and buggy to rerun with same
		// version of stateDB that has been marked conflict.
		// Therefore, treat it as conflict and rerun, leave the result to conflict check.
		// Load conflict as it maybe updated by conflict checker or other execution slots.
		// use old mIndex so that we can try the new one that is updated by other thread of merging
		// during execution.
		conflictIndex = txReq.conflictIndex.Load()
		if conflictIndex < mIndex {
			if txReq.conflictIndex.CompareAndSwap(conflictIndex, mIndex) {
				log.Debug(fmt.Sprintf("Update conflictIndex in execution because of error: %s, new conflictIndex: %d", err.Error(), conflictIndex))
			}
		}
		atomic.CompareAndSwapInt32(&txReq.runnable, 0, 1)
		// the error could be caused by unconfirmed balance reference,
		// the balance could insufficient to pay its gas limit, which cause it preCheck.buyGas() failed
		// redo could solve it.
		log.Debug("In slot execution error", "error", err,
			"slotIndex", slotIndex, "txIndex", txReq.txIndex)
	}
	p.unconfirmedResults.Store(txReq.txIndex, &txResult)
	return &txResult
}

// toConfirmTxIndex confirm a serial TxResults with same txIndex
func (p *ParallelStateProcessor) toConfirmTxIndex(targetTxIndex int, isStage2 bool) *ParallelTxResult {
	if isStage2 {
		if targetTxIndex <= int(p.mergedTxIndex.Load())+1 {
			// `p.mergedTxIndex+1` is the one to be merged,
			// in stage2, we do likely conflict check, for these not their turn.
			return nil
		}
	}

	for {
		// handle a targetTxIndex in a loop
		var targetResult *ParallelTxResult
		if isStage2 {
			result, ok := p.unconfirmedResults.Load(targetTxIndex)
			if !ok {
				return nil
			}
			targetResult = result.(*ParallelTxResult)

			// in stage 2, don't schedule a new redo if the TxReq is:
			//  a.runnable: it will be redone
			//  b.running: the new result will be more reliable, we skip check right now
			if atomic.LoadInt32(&targetResult.txReq.runnable) == 1 {
				return nil
			}
			if targetResult.executedIndex < targetResult.txReq.executedNum.Load() {
				// skip the intermediate result that is not the latest.
				return nil
			}
		} else {
			// pop one result as target result.
			result, ok := p.pendingConfirmResults.LoadAndDelete(targetTxIndex)
			if !ok {
				return nil
			}
			targetResult = result.(*ParallelTxResult)
		}

		valid := p.toConfirmTxIndexResult(targetResult, isStage2)
		if !valid {
			staticSlotIndex := targetResult.txReq.staticSlotIndex
			conflictBase := targetResult.slotDB.BaseTxIndex()
			conflictIndex := targetResult.txReq.conflictIndex.Load()
			if conflictIndex < int32(conflictBase) {
				if targetResult.txReq.conflictIndex.CompareAndSwap(conflictIndex, int32(conflictBase)) {
					log.Debug("Update conflict index", "conflictIndex", conflictIndex, "conflictBase", conflictBase)
				}
			}
			if isStage2 {
				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt the slot's current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
				return nil
			}

			if _, ok := p.pendingConfirmResults.Load(targetTxIndex); !ok { // this is the last result to check, and it is not valid
				// This means that the tx has been executed more than blockTxCount times, so it exits with the error.
				if targetResult.txReq.txIndex == int(p.mergedTxIndex.Load())+1 &&
					targetResult.slotDB.BaseTxIndex() == int(p.mergedTxIndex.Load()) {
					if targetResult.err != nil {
						if false { // TODO: delete the printf
							fmt.Printf("!!!!!!!!!!! Parallel execution exited with error!!!!!, txIndex:%d, err: %v\n", targetResult.txReq.txIndex, targetResult.err)
						}
						return targetResult
					} else {
						// abnormal exit with conflict error, need check the parallel algorithm
						targetResult.err = ErrParallelUnexpectedConflict
						if false {
							fmt.Printf("!!!!!!!!!!! Parallel execution exited unexpected conflict!!!!!, txIndex:%d\n", targetResult.txReq.txIndex)
						}
						return targetResult
					}
					//}
				}
				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt its current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
				// reclaim the result.
				targetResult.slotDB.PutSyncPool()
				return nil
			}
			continue
		}
		if isStage2 {
			// likely valid, but not sure, can not deliver
			return nil
		}
		return targetResult
	}
}

// to confirm one txResult, return true if the result is valid
// if it is in Stage 2 it is a likely result, not 100% sure
func (p *ParallelStateProcessor) toConfirmTxIndexResult(txResult *ParallelTxResult, isStage2 bool) bool {
	txReq := txResult.txReq
	if p.hasConflict(txResult, isStage2) {
		log.Info(fmt.Sprintf("HasConflict!! block: %d, txIndex: %d\n", txResult.txReq.block.NumberU64(), txResult.txReq.txIndex))
		return false
	}
	if isStage2 { // not its turn
		return true // likely valid, not sure, not finalized right now.
	}

	// goroutine unsafe operation will be handled from here for safety
	gasConsumed := txReq.gasLimit - txResult.gpSlot.Gas()
	if gasConsumed != txResult.result.UsedGas {
		log.Error("gasConsumed != result.UsedGas mismatch",
			"gasConsumed", gasConsumed, "result.UsedGas", txResult.result.UsedGas)
	}

	// ok, time to do finalize, stage2 should not be parallel
	txResult.receipt, txResult.err = applyTransactionStageFinalization(txResult.evm, txResult.result,
		*txReq.msg, p.config, txResult.slotDB, txReq.block,
		txReq.tx, txReq.usedGas, txResult.originalNonce)
	return true
}

func (p *ParallelStateProcessor) runSlotLoop(slotIndex int, slotType int32) {
	curSlot := p.slotState[slotIndex]
	var wakeupChan chan struct{}
	var stopChan chan struct{}

	if slotType == parallelPrimarySlot {
		wakeupChan = curSlot.primaryWakeUpChan
		stopChan = curSlot.primaryStopChan
	} else {
		wakeupChan = curSlot.shadowWakeUpChan
		stopChan = curSlot.shadowStopChan
	}

	lastStartPos := 0
	for {
		select {
		case <-stopChan:
			p.stopSlotChan <- struct{}{}
			continue
		case <-wakeupChan:
		}

		interrupted := false

		for i := lastStartPos; i < len(curSlot.pendingTxReqList); i++ {
			// for i, txReq := range curSlot.pendingTxReqList {
			txReq := curSlot.pendingTxReqList[i]
			if txReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}
			lastStartPos = i

			if txReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}

			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				break
			}

			// first try next to be merged req.
			nextIdx := p.mergedTxIndex.Load() + 1
			if nextIdx < int32(len(p.allTxReqs)) {
				nextMergeReq := p.allTxReqs[nextIdx]
				if nextMergeReq.runnable == 1 {
					if atomic.CompareAndSwapInt32(&nextMergeReq.runnable, 1, 0) {
						// execute.
						res := p.executeInSlot(slotIndex, nextMergeReq)
						if res != nil {
							p.txResultChan <- res
						}
					}
				}
			}

			if txReq.runnable == 1 {
				// try the next req in loop sequence.
				if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, txReq)
				if res == nil {
					continue
				}
				p.txResultChan <- res
			}
		}
		// switched to the other slot.
		if interrupted {
			continue
		}

		// txReq in this Slot have all been executed, try steal one from other slot.
		// as long as the TxReq is runnable, we steal it, mark it as stolen

		for j := int(p.mergedTxIndex.Load()) + 1; j < len(p.allTxReqs); j++ {
			stealTxReq := p.allTxReqs[j]
			if stealTxReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}

			if stealTxReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}

			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				break
			}

			// first try next to be merged req.
			nextIdx := p.mergedTxIndex.Load() + 1
			if nextIdx < int32(len(p.allTxReqs)) {
				nextMergeReq := p.allTxReqs[nextIdx]
				if nextMergeReq.runnable == 1 {
					if atomic.CompareAndSwapInt32(&nextMergeReq.runnable, 1, 0) {
						// execute.
						res := p.executeInSlot(slotIndex, nextMergeReq)
						if res != nil {
							p.txResultChan <- res
						}
					}
				}
			}

			if stealTxReq.runnable == 1 {
				if !atomic.CompareAndSwapInt32(&stealTxReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, stealTxReq)
				if res == nil {
					continue
				}
				p.txResultChan <- res
			}
		}
	}
}

func (p *ParallelStateProcessor) runQuickMergeSlotLoop(slotIndex int, slotType int32) {
	curSlot := p.slotState[slotIndex]
	var wakeupChan chan struct{}
	var stopChan chan struct{}

	if slotType == parallelPrimarySlot {
		wakeupChan = curSlot.primaryWakeUpChan
		stopChan = curSlot.primaryStopChan
	} else {
		wakeupChan = curSlot.shadowWakeUpChan
		stopChan = curSlot.shadowStopChan
	}
	for {
		select {
		case <-stopChan:
			p.stopSlotChan <- struct{}{}
			continue
		case <-wakeupChan:
		}

		next := int(p.mergedTxIndex.Load()) + 1

		executed := 5
		for i := next; i < len(p.allTxReqs); i++ {
			txReq := p.allTxReqs[next]
			if executed == 0 {
				break
			}
			if txReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}
			if txReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}
			if txReq.runnable == 1 {
				if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, txReq)
				if res != nil {
					executed--
					p.txResultChan <- res
				}
			}
		}
	}
}

func (p *ParallelStateProcessor) runConfirmStage2Loop() {
	for {
		select {
		case <-p.stopConfirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				<-p.confirmStage2Chan
			}
			p.stopSlotChan <- struct{}{}
			continue
		case <-p.confirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				<-p.confirmStage2Chan // drain the chan to get the latest merged txIndex
			}
		}
		// stage 2,if all tx have been executed at least once, and its result has been received.
		// in Stage 2, we will run check when merge is advanced.
		// more aggressive tx result confirm, even for these Txs not in turn
		startTxIndex := int(p.mergedTxIndex.Load()) + 2 // stage 2's will start from the next target merge index
		endTxIndex := startTxIndex + stage2CheckNumber
		txSize := len(p.allTxReqs)
		if endTxIndex > (txSize - 1) {
			endTxIndex = txSize - 1
		}
		log.Debug("runConfirmStage2Loop", "startTxIndex", startTxIndex, "endTxIndex", endTxIndex)
		for txIndex := startTxIndex; txIndex < endTxIndex; txIndex++ {
			p.toConfirmTxIndex(txIndex, true)
		}
		// make sure all slots are wake up
		for i := 0; i < p.parallelNum; i++ {
			p.switchSlot(i)
		}
	}
}

func (p *ParallelStateProcessor) handleTxResults() *ParallelTxResult {
	confirmedResult := p.toConfirmTxIndex(int(p.mergedTxIndex.Load())+1, false)
	if confirmedResult == nil {
		return nil
	}
	// schedule stage 2 when new Tx has been merged, schedule once and ASAP
	// stage 2,if all tx have been executed at least once, and its result has been received.
	// in Stage 2, we will run check when main DB is advanced, i.e., new Tx result has been merged.
	if p.inConfirmStage2 && int(p.mergedTxIndex.Load()) >= p.nextStage2TxIndex {
		p.nextStage2TxIndex = int(p.mergedTxIndex.Load()) + stage2CheckNumber
		p.confirmStage2Chan <- int(p.mergedTxIndex.Load())
	}
	return confirmedResult
}

// wait until the next Tx is executed and its result is merged to the main stateDB
func (p *ParallelStateProcessor) confirmTxResults(statedb *state.StateDB, gp *GasPool) *ParallelTxResult {
	result := p.handleTxResults()
	if result == nil {
		return nil
	}
	// ok, the tx result is valid and can be merged
	if result.err != nil {
		return result
	}

	if err := gp.SubGas(result.receipt.GasUsed); err != nil {
		log.Error("gas limit reached", "block", result.txReq.block.Number(),
			"txIndex", result.txReq.txIndex, "GasUsed", result.receipt.GasUsed, "gp.Gas", gp.Gas())
	}

	resultTxIndex := result.txReq.txIndex

	var root []byte
	header := result.txReq.block.Header()

	isByzantium := p.config.IsByzantium(header.Number)
	isEIP158 := p.config.IsEIP158(header.Number)
	result.slotDB.FinaliseForParallel(isByzantium || isEIP158, statedb)

	// merge slotDB into mainDB
	statedb.MergeSlotDB(result.slotDB, result.receipt, resultTxIndex, result.result.delayFees)

	delayGasFee := result.result.delayFees
	// add delayed gas fee
	if delayGasFee != nil {
		if delayGasFee.TipFee != nil {
			statedb.AddBalance(delayGasFee.Coinbase, delayGasFee.TipFee)
		}
		if delayGasFee.BaseFee != nil {
			statedb.AddBalance(params.OptimismBaseFeeRecipient, delayGasFee.BaseFee)
		}
		if delayGasFee.L1Fee != nil {
			statedb.AddBalance(params.OptimismL1FeeRecipient, delayGasFee.L1Fee)
		}
	}

	// Do IntermediateRoot after mergeSlotDB.
	if !isByzantium {
		root = statedb.IntermediateRoot(isEIP158).Bytes()
	}
	result.receipt.PostState = root

	if resultTxIndex != int(p.mergedTxIndex.Load())+1 {
		log.Error("ProcessParallel tx result out of order", "resultTxIndex", resultTxIndex,
			"p.mergedTxIndex", p.mergedTxIndex.Load())
	}
	p.mergedTxIndex.Store(int32(resultTxIndex))

	// trigger all slot to run left conflicted txs
	for _, slot := range p.slotState {
		var wakeupChan chan struct{}
		if slot.activatedType == parallelPrimarySlot {
			wakeupChan = slot.primaryWakeUpChan
		} else {
			wakeupChan = slot.shadowWakeUpChan
		}
		select {
		case wakeupChan <- struct{}{}:
		default:
		}
	}
	// schedule prefetch once only when unconfirmedResult is valid
	if result.err == nil {
		if _, ok := p.txReqExecuteRecord[resultTxIndex]; !ok {
			p.txReqExecuteRecord[resultTxIndex] = 0
			p.txReqExecuteCount++
			statedb.AddrPrefetch(result.slotDB)
			if !p.inConfirmStage2 && p.txReqExecuteCount == p.targetStage2Count {
				p.inConfirmStage2 = true
			}
		}
		p.txReqExecuteRecord[resultTxIndex]++
	}
	// after merge, the slotDB will not accessible, reclaim the resource
	result.slotDB.PutSyncPool()
	return result
}

func (p *ParallelStateProcessor) doCleanUp() {
	// 1.clean up all slot: primary and shadow, to make sure they are stopped
	for _, slot := range p.slotState {
		slot.primaryStopChan <- struct{}{}
		slot.shadowStopChan <- struct{}{}
		<-p.stopSlotChan
		<-p.stopSlotChan
	}
	// 2.discard delayed txResults if any
	for {
		if len(p.txResultChan) > 0 {
			<-p.txResultChan
			continue
		}
		break
	}
	// 3.make sure the confirmation routine is stopped
	p.stopConfirmStage2Chan <- struct{}{}
	<-p.stopSlotChan
}

// Process implements BEP-130 Parallel Transaction Execution
func (p *ParallelStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		usedGas = new(uint64)
		header  = block.Header()
		gp      = new(GasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	if p.config.PreContractForkBlock != nil && p.config.PreContractForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyPreContractHardFork(statedb)
	}

	misc.EnsureCreate2Deployer(p.config, block.Time(), statedb)

	allTxs := block.Transactions()
	p.resetState(len(allTxs), statedb)

	var (
		// with parallel mode, vmenv will be created inside of slot
		blockContext = NewEVMBlockContext(block.Header(), p.bc, nil, p.config, statedb)
		vmenv        = vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
		signer       = types.MakeSigner(p.bc.chainConfig, block.Number(), block.Time())
	)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}
	statedb.MarkFullProcessed()
	txDAG := cfg.TxDAG

	txNum := len(allTxs)
	latestExcludedTx := -1
	// Iterate over and process the individual transactions
	p.commonTxs = make([]*types.Transaction, 0, txNum)
	p.receipts = make([]*types.Receipt, 0, txNum)

	for i, tx := range allTxs {
		// can be moved it into slot for efficiency, but signer is not concurrent safe
		// Parallel Execution 1.0&2.0 is for full sync mode, Nonce PreCheck is not necessary
		// And since we will do out-of-order execution, the Nonce PreCheck could fail.
		// We will disable it and leave it to Parallel 3.0 which is for validator mode
		msg, err := TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		// find the latestDepTx from TxDAG or latestExcludedTx
		latestDepTx := -1
		if dep := types.TxDependency(txDAG, i); len(dep) > 0 {
			latestDepTx = int(dep[len(dep)-1])
		}
		if latestDepTx < latestExcludedTx {
			latestDepTx = latestExcludedTx
		}

		// parallel start, wrap an exec message, which will be dispatched to a slot
		txReq := &ParallelTxRequest{
			txIndex:         i,
			baseStateDB:     statedb,
			staticSlotIndex: -1,
			tx:              tx,
			gasLimit:        block.GasLimit(), // gp.Gas().
			msg:             msg,
			block:           block,
			vmConfig:        cfg,
			usedGas:         usedGas,
			curTxChan:       make(chan int, 1),
			runnable:        1, // 0: not runnable, 1: runnable
			useDAG:          txDAG != nil,
		}
		txReq.executedNum.Store(0)
		txReq.conflictIndex.Store(-2)
		if latestDepTx >= 0 {
			txReq.conflictIndex.Store(int32(latestDepTx))
		}
		p.allTxReqs = append(p.allTxReqs, txReq)
		if txDAG != nil && txDAG.TxDep(i).CheckFlag(types.ExcludedTxFlag) {
			latestExcludedTx = i
		}
	}
	allTxCount := len(p.allTxReqs)
	// set up stage2 enter criteria
	p.targetStage2Count = allTxCount
	if p.targetStage2Count > 50 {
		// usually, the last Tx could be the bottleneck it could be very slow,
		// so it is better for us to enter stage 2 a bit earlier
		p.targetStage2Count = p.targetStage2Count - stage2AheadNum
	}

	p.delayGasFee = false
	p.doStaticDispatch(p.allTxReqs)
	if txDAG != nil && txDAG.DelayGasFeeDistribution() {
		p.delayGasFee = true
	}

	// after static dispatch, we notify the slot to work.
	for _, slot := range p.slotState {
		slot.primaryWakeUpChan <- struct{}{}
	}

	// kick off the result handler.
	p.resultProcessChan <- &ResultHandleEnv{statedb: statedb, gp: gp, txCount: allTxCount}
	for {
		if int(p.mergedTxIndex.Load())+1 == allTxCount {
			// put it ahead of chan receive to avoid waiting for empty block
			break
		}
		unconfirmedResult := <-p.txResultChan
		if unconfirmedResult.txReq == nil && int(p.mergedTxIndex.Load())+1 == allTxCount {
			// all tx results are merged.
			break
		}

		unconfirmedTxIndex := unconfirmedResult.txReq.txIndex
		if unconfirmedTxIndex <= int(p.mergedTxIndex.Load()) {
			log.Debug("drop merged txReq", "unconfirmedTxIndex", unconfirmedTxIndex, "p.mergedTxIndex", p.mergedTxIndex.Load())
			continue
		}
		prevResult, ok := p.pendingConfirmResults.Load(unconfirmedTxIndex)
		if !ok || prevResult.(*ParallelTxResult).slotDB.BaseTxIndex() < unconfirmedResult.slotDB.BaseTxIndex() {
			p.pendingConfirmResults.Store(unconfirmedTxIndex, unconfirmedResult)
			p.resultAppendChan <- struct{}{}
		}
	}

	// clean up when the block is processed
	p.doCleanUp()

	if p.error != nil {
		return nil, nil, 0, p.error
	}

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	// all txs have been merged at this point, no need to acquire the lock of commonTxs
	if p.mergedTxIndex.Load() >= 0 && p.debugConflictRedoNum > 0 {
		log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
			"txNum", txNum,
			"len(commonTxs)", len(p.commonTxs),
			"conflictNum", p.debugConflictRedoNum,
			"redoRate(%)", 100*(p.debugConflictRedoNum)/len(p.commonTxs),
			"txDAG", txDAG != nil)
	}
	if metrics.EnabledExpensive {
		parallelTxNumMeter.Mark(int64(len(p.commonTxs)))
		parallelConflictTxNumMeter.Mark(int64(p.debugConflictRedoNum))
	}

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Number(), block.Time()) {
		return nil, nil, 0, errors.New("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, p.commonTxs, block.Uncles(), withdrawals)

	var allLogs []*types.Log
	for _, receipt := range p.receipts {
		allLogs = append(allLogs, receipt.Logs...)
	}
	return p.receipts, allLogs, *usedGas, nil
}

func (p *ParallelStateProcessor) handlePendingResultLoop() {
	var info *ResultHandleEnv
	var stateDB *state.StateDB
	var gp *GasPool
	var txCount int
	for {
		select {
		case info = <-p.resultProcessChan:
			stateDB = info.statedb
			gp = info.gp
			txCount = info.txCount
			log.Debug("handlePendingResult get Env", "stateDBTx", stateDB.TxIndex(), "gp", gp.String(), "txCount", txCount)
		case <-p.resultAppendChan:
		}

		// if all merged, notify the main routine. continue to wait for next block.
		if p.error != nil || p.mergedTxIndex.Load()+1 == int32(txCount) {
			// log.Info("handlePendingResult merged all")
			p.txResultChan <- &ParallelTxResult{txReq: nil, result: nil}
			// clear the pending chan.
			for len(p.resultAppendChan) > 0 {
				<-p.resultAppendChan
			}
			continue
		}
		// busy waiting.
		for {
			nextTxIndex := int(p.mergedTxIndex.Load()) + 1
			if p.error != nil || nextTxIndex == txCount {
				p.txResultChan <- &ParallelTxResult{txReq: nil, result: nil}
				// clear the pending chan.
				for len(p.resultAppendChan) > 0 {
					<-p.resultAppendChan
				}
				break
			}
			if _, ok := p.pendingConfirmResults.Load(nextTxIndex); !ok {
				break
			}
			log.Debug("Start to check result", "TxIndex", int(nextTxIndex), "stateDBTx", stateDB.TxIndex(), "gp", gp.String())

			result := p.confirmTxResults(stateDB, gp)
			if result == nil {
				break
			} else {
				log.Debug("in Confirm Loop - after confirmTxResults",
					"mergedIndex", p.mergedTxIndex.Load(),
					"confirmedIndex", result.txReq.txIndex,
					"result.err", result.err)
			}
			p.resultMutex.Lock()
			// update tx result
			if result.err != nil {
				log.Error("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
					"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
				p.error = fmt.Errorf("could not apply tx %d [%v]: %w", result.txReq.txIndex, result.txReq.tx.Hash().Hex(), result.err)
				p.resultMutex.Unlock()
				continue
			}
			p.commonTxs = append(p.commonTxs, result.txReq.tx)
			p.receipts = append(p.receipts, result.receipt)
			p.resultMutex.Unlock()
		}
	}
}

func applyTransactionStageExecution(msg *Message, gp *GasPool, statedb *state.ParallelStateDB, evm *vm.EVM, delayGasFee bool) (*vm.EVM, *ExecutionResult, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	var (
		result *ExecutionResult
		err    error
	)
	if delayGasFee {
		result, err = ApplyMessageDelayGasFee(evm, msg, gp)
	} else {
		result, err = ApplyMessage(evm, msg, gp)
	}

	if err != nil {
		return nil, nil, err
	}

	return evm, result, err
}

func applyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg Message, config *params.ChainConfig, statedb *state.ParallelStateDB, block *types.Block, tx *types.Transaction, usedGas *uint64, nonce *uint64) (*types.Receipt, error) {

	*usedGas += result.UsedGas
	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: nil, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	if msg.IsDepositTx && config.IsOptimismRegolith(evm.Context.Time) {
		// The actual nonce for deposit transactions is only recorded from Regolith onwards and
		// otherwise must be nil.
		receipt.DepositNonce = nonce
		// The DepositReceiptVersion for deposit transactions is only recorded from Canyon onwards
		// and otherwise must be nil.
		if config.IsOptimismCanyon(evm.Context.Time) {
			receipt.DepositReceiptVersion = new(uint64)
			*receipt.DepositReceiptVersion = types.CanyonDepositReceiptVersion
		}
	}
	if tx.Type() == types.BlobTxType {
		receipt.BlobGasUsed = uint64(len(tx.BlobHashes()) * params.BlobTxBlobGasPerBlob)
		receipt.BlobGasPrice = evm.Context.BlobBaseFee
	}
	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, *nonce)
	}
	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), block.NumberU64(), block.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = block.Hash()
	receipt.BlockNumber = block.Number()
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, nil
}
