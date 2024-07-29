package core

import (
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	parallelPrimarySlot = 0
	parallelShadowSlot  = 1
	stage2CheckNumber   = 30 // ConfirmStage2 will check this number of transaction, to avoid too busy stage2 check
	stage2AheadNum      = 3  // enter ConfirmStage2 in advance to avoid waiting for Fat Tx
)

type ParallelStateProcessor struct {
	StateProcessor
	parallelNum           int          // leave a CPU to dispatcher
	slotState             []*SlotState // idle, or pending messages
	allTxReqs             []*ParallelTxRequest
	txResultChan          chan *ParallelTxResult      // to notify dispatcher that a tx is done
	mergedTxIndex         int                         // the latest finalized tx index, fixme: use Atomic
	pendingConfirmResults map[int][]*ParallelTxResult // tx could be executed several times, with several result to check
	unconfirmedResults    *sync.Map                   // this is for stage2 confirm, since pendingConfirmResults can not be accessed in stage2 loop
	unconfirmedDBs        *sync.Map
	slotDBsToRelease      []*state.ParallelStateDB
	stopSlotChan          chan struct{}
	stopConfirmChan       chan struct{}
	debugConflictRedoNum  int
	// start for confirm stage2
	confirmStage2Chan     chan int
	stopConfirmStage2Chan chan struct{}
	txReqExecuteRecord    map[int]int
	txReqExecuteCount     int
	inConfirmStage2       bool
	targetStage2Count     int // when executed txNUM reach it, enter stage2 RT confirm
	nextStage2TxIndex     int
}

func NewParallelStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine, parallelNum int) *ParallelStateProcessor {
	processor := &ParallelStateProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		parallelNum:    parallelNum,
	}
	processor.init()
	return processor
}

type MergedTxInfo struct {
	slotDB              *state.StateDB // used for SlotDb reuse only, otherwise, it can be discarded
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
	executedIndex int32 // the TxReq can be executed several time, increase index for each execution
	slotIndex     int   // slot index
	txReq         *ParallelTxRequest
	receipt       *types.Receipt
	slotDB        *state.ParallelStateDB // if updated, it is not equal to txReq.slotDB
	gpSlot        *GasPool
	evm           *vm.EVM
	result        *ExecutionResult
	originalNonce *uint64
	err           error
}

type ParallelTxRequest struct {
	txIndex         int
	baseStateDB     *state.StateDB
	staticSlotIndex int // static dispatched id
	tx              *types.Transaction
	gasLimit        uint64
	msg             *Message
	block           *types.Block
	vmConfig        vm.Config
	usedGas         *uint64
	curTxChan       chan int
	systemAddrRedo  bool
	runnable        int32 // 0: not runnable, 1: runnable
	executedNum     int32
	retryNum        int32
}

// to create and start the execution slot goroutines
func (p *ParallelStateProcessor) init() {
	log.Info("Parallel execution mode is enabled", "Parallel Num", p.parallelNum,
		"CPUNum", runtime.NumCPU())
	p.txResultChan = make(chan *ParallelTxResult, 200)
	p.stopSlotChan = make(chan struct{}, 1)
	p.stopConfirmChan = make(chan struct{}, 1)
	p.stopConfirmStage2Chan = make(chan struct{}, 1)

	p.slotState = make([]*SlotState, p.parallelNum)
	for i := 0; i < p.parallelNum; i++ {
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
			p.runSlotLoop(slotIndex, parallelShadowSlot) // this loop will be permanent live
		}(i)

	}

	p.confirmStage2Chan = make(chan int, 10)
	go func() {
		p.runConfirmStage2Loop() // this loop will be permanent live
	}()
}

// clear slot state for each block.
func (p *ParallelStateProcessor) resetState(txNum int, statedb *state.StateDB) {
	if txNum == 0 {
		return
	}
	p.mergedTxIndex = -1
	p.debugConflictRedoNum = 0
	p.inConfirmStage2 = false

	statedb.PrepareForParallel()
	p.allTxReqs = make([]*ParallelTxRequest, 0)
	p.slotDBsToRelease = make([]*state.ParallelStateDB, 0, txNum)

	stateDBsToRelease := p.slotDBsToRelease
	go func() {
		for _, slotDB := range stateDBsToRelease {
			slotDB.PutSyncPool()
		}
	}()
	for _, slot := range p.slotState {
		slot.pendingTxReqList = make([]*ParallelTxRequest, 0)
		slot.activatedType = parallelPrimarySlot
	}
	p.unconfirmedResults = new(sync.Map)
	p.unconfirmedDBs = new(sync.Map)
	p.pendingConfirmResults = make(map[int][]*ParallelTxResult, 200)
	p.txReqExecuteRecord = make(map[int]int, 200)
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
			// first: same From are all in same slot
			slotIndex = i
		} else if txReq.msg.To != nil {
			// To Address, with txIndex sorted, could be in different slot.
			if i, ok := toSlotMap[*txReq.msg.To]; ok {
				slotIndex = i
			}
		}

		// not found, dispatch to most hungry slot
		if slotIndex == -1 {
			var workload = len(p.slotState[0].pendingTxReqList)
			slotIndex = 0
			for i, slot := range p.slotState { // can start from index 1
				if len(slot.pendingTxReqList) < workload {
					slotIndex = i
					workload = len(slot.pendingTxReqList)
				}
			}
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

// do conflict detect
func (p *ParallelStateProcessor) hasConflict(txResult *ParallelTxResult, isStage2 bool) bool {
	slotDB := txResult.slotDB
	if txResult.err != nil {
		return true
	} else if slotDB.NeedsRedo() {
		// if this is any reason that indicates this transaction needs to redo, skip the conflict check
		return true
	} else {
		// to check if what the slot db read is correct.
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
			slot.shadowWakeUpChan <- struct{}{} // only notify when target once
		}
	} else if atomic.CompareAndSwapInt32(&slot.activatedType, parallelShadowSlot, parallelPrimarySlot) {
		// switch from shadow to normal slot
		if len(slot.primaryWakeUpChan) == 0 {
			slot.primaryWakeUpChan <- struct{}{} // only notify when target once
		}
	}
}

func (p *ParallelStateProcessor) executeInSlot(slotIndex int, txReq *ParallelTxRequest) *ParallelTxResult {
	atomic.AddInt32(&txReq.executedNum, 1)
	slotDB := state.NewSlotDB(txReq.baseStateDB, txReq.txIndex, p.mergedTxIndex, p.unconfirmedDBs)

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
		on = txReq.baseStateDB.GetNonce(txReq.msg.From)
	}

	slotDB.SetTxContext(txReq.tx.Hash(), txReq.txIndex)

	evm, result, err := applyTransactionStageExecution(txReq.msg, gpSlot, slotDB, vmenv)
	txResult := ParallelTxResult{
		executedIndex: atomic.LoadInt32(&txReq.executedNum),
		slotIndex:     slotIndex,
		txReq:         txReq,
		receipt:       nil, // receipt is generated in finalize stage
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

// to confirm a serial TxResults with same txIndex
func (p *ParallelStateProcessor) toConfirmTxIndex(targetTxIndex int, isStage2 bool) *ParallelTxResult {
	if isStage2 {
		if targetTxIndex <= p.mergedTxIndex+1 {
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
			if targetResult.executedIndex < atomic.LoadInt32(&targetResult.txReq.executedNum) {
				// skip the intermediate result that is not the latest.
				return nil
			}
		} else {
			// pop one result as target result.
			results := p.pendingConfirmResults[targetTxIndex]
			resultsLen := len(results)
			if resultsLen == 0 { // there is no pending result can be verified, break and wait for incoming results
				return nil
			}
			targetResult = results[len(results)-1]
			// last is the freshest, stack based priority
			p.pendingConfirmResults[targetTxIndex] = p.pendingConfirmResults[targetTxIndex][:resultsLen-1] // remove from the queue
		}

		valid := p.toConfirmTxIndexResult(targetResult, isStage2)
		if !valid {
			staticSlotIndex := targetResult.txReq.staticSlotIndex // it is better to run the TxReq in its static dispatch slot
			if isStage2 {
				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt the slot's current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
				return nil
			}

			if len(p.pendingConfirmResults[targetTxIndex]) == 0 { // this is the last result to check, and it is not valid
				blockTxCount := targetResult.txReq.block.Transactions().Len()
				// This means that the tx has been executed more than blockTxCount times, so it exits with the error.
				// TODO-dav: p.mergedTxIndex+2 may be more reasonable? - this is buggy for expected exit
				if targetResult.txReq.txIndex == p.mergedTxIndex+1 {
					// txReq is the next to merge
					if atomic.LoadInt32(&targetResult.txReq.retryNum) <= int32(blockTxCount)+3000 {
						atomic.AddInt32(&targetResult.txReq.retryNum, 1)
						// conflict retry
					} else {
						// retry 100 times and still conflict, either the tx is expected to be wrong, or something wrong.
						if targetResult.err != nil {
							fmt.Printf("!!!!!!!!!!! Parallel execution exited with error!!!!!, txIndex:%d, err: %v\n", targetResult.txReq.txIndex, targetResult.err)
							return targetResult
						} else {
							// abnormal exit with conflict error, need check the parallel algorithm
							targetResult.err = ErrParallelUnexpectedConflict

							fmt.Printf("!!!!!!!!!!! Parallel execution exited unexpected conflict!!!!!, txIndex:%d\n", targetResult.txReq.txIndex)

							return targetResult
						}
					}
				}
				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt its current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
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
		log.Debug("HasConflict!! block: %d, txIndex: %d\n", txResult.txReq.block.NumberU64(), txResult.txReq.txIndex)
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
	header := txReq.block.Header()
	txResult.receipt, txResult.err = applyTransactionStageFinalization(txResult.evm, txResult.result,
		*txReq.msg, p.config, txResult.slotDB, header,
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
	for {
		select {
		case <-stopChan:
			p.stopSlotChan <- struct{}{}
			continue
		case <-wakeupChan:
		}

		interrupted := false
		for _, txReq := range curSlot.pendingTxReqList {
			if txReq.txIndex <= p.mergedTxIndex {
				continue
			}

			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				// fmt.Printf("Dav -- runInLoop, - activatedType - TxREQ: %d\n", txReq.txIndex)

				break
			}
			if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
				// not swapped: txReq.runnable == 0
				//fmt.Printf("Dav -- runInLoop, - not runnable - TxREQ: %d\n", txReq.txIndex)
				continue
			}
			// fmt.Printf("Dav -- runInLoop, - executeInSlot - TxREQ: %d\n", txReq.txIndex)
			p.txResultChan <- p.executeInSlot(slotIndex, txReq)
			// fmt.Printf("Dav -- runInLoop, - loopbody tail - TxREQ: %d\n", txReq.txIndex)
		}
		// switched to the other slot.
		if interrupted {
			continue
		}

		// txReq in this Slot have all been executed, try steal one from other slot.
		// as long as the TxReq is runnable, we steal it, mark it as stolen
		for _, stealTxReq := range p.allTxReqs {
			// fmt.Printf("Dav -- stealLoop, handle TxREQ: %d\n", stealTxReq.txIndex)
			if stealTxReq.txIndex <= p.mergedTxIndex {
				// fmt.Printf("Dav -- stealLoop, - txReq.txIndex <= p.mergedTxIndex - TxREQ: %d\n", stealTxReq.txIndex)
				continue
			}
			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				// fmt.Printf("Dav -- stealLoop, - activatedType - TxREQ: %d\n", stealTxReq.txIndex)

				break
			}

			if !atomic.CompareAndSwapInt32(&stealTxReq.runnable, 1, 0) {
				// not swapped: txReq.runnable == 0
				// fmt.Printf("Dav -- stealLoop, - not runnable - TxREQ: %d\n", stealTxReq.txIndex)

				continue
			}
			// fmt.Printf("Dav -- stealLoop, - executeInSlot - TxREQ: %d\n", stealTxReq.txIndex)
			p.txResultChan <- p.executeInSlot(slotIndex, stealTxReq)
			// fmt.Printf("Dav -- stealLoop, - loopbody tail - TxREQ: %d\n", stealTxReq.txIndex)
		}
	}
}

func (p *ParallelStateProcessor) runConfirmStage2Loop() {
	for {
		// var mergedTxIndex int
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
		// now we will be more aggressive:
		//   do conflict check , as long as tx result is generated,
		//   if lucky, it is the Tx's turn, we will do conflict check with WBNB makeup
		//   otherwise, do conflict check without WBNB makeup, but we will ignore WBNB's balance conflict.
		// throw these likely conflicted tx back to re-execute
		startTxIndex := p.mergedTxIndex + 2 // stage 2's will start from the next target merge index
		endTxIndex := startTxIndex + stage2CheckNumber
		txSize := len(p.allTxReqs)
		if endTxIndex > (txSize - 1) {
			endTxIndex = txSize - 1
		}
		log.Debug("runConfirmStage2Loop", "startTxIndex", startTxIndex, "endTxIndex", endTxIndex)
		// conflictNumMark := p.debugConflictRedoNum
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
	confirmedResult := p.toConfirmTxIndex(p.mergedTxIndex+1, false)
	if confirmedResult == nil {
		return nil
	}
	// schedule stage 2 when new Tx has been merged, schedule once and ASAP
	// stage 2,if all tx have been executed at least once, and its result has been received.
	// in Stage 2, we will run check when main DB is advanced, i.e., new Tx result has been merged.
	if p.inConfirmStage2 && p.mergedTxIndex >= p.nextStage2TxIndex {
		p.nextStage2TxIndex = p.mergedTxIndex + stage2CheckNumber
		p.confirmStage2Chan <- p.mergedTxIndex
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
	if p.config.IsByzantium(header.Number) {
		result.slotDB.FinaliseForParallel(true, statedb)
	} else {
		root = result.slotDB.IntermediateRootForSlotDB(p.config.IsEIP158(header.Number), statedb).Bytes()
	}
	result.receipt.PostState = root
	// merge slotDB into mainDB
	statedb.MergeSlotDB(result.slotDB, result.receipt, resultTxIndex)

	if resultTxIndex != p.mergedTxIndex+1 {
		log.Error("ProcessParallel tx result out of order", "resultTxIndex", resultTxIndex,
			"p.mergedTxIndex", p.mergedTxIndex)
	}
	p.mergedTxIndex = resultTxIndex

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
		if len(p.txResultChan) > 0 { // drop prefetch addr?
			<-p.txResultChan
			continue
		}
		break
	}
	// 3.make sure the confirmation routine is stopped
	p.stopConfirmStage2Chan <- struct{}{}
	<-p.stopSlotChan
}

// Implement BEP-130: Parallel Transaction Execution.
func (p *ParallelStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		gp       = new(GasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	if p.config.PreContractForkBlock != nil && p.config.PreContractForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyPreContractHardFork(statedb)
	}

	txNum := len(block.Transactions())
	p.resetState(txNum, statedb)

	// Iterate over and process the individual transactions
	commonTxs := make([]*types.Transaction, 0, txNum)

	var (
		// with parallel mode, vmenv will be created inside of slot
		blockContext = NewEVMBlockContext(block.Header(), p.bc, nil, p.config, statedb)
		vmenv        = vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
		signer       = types.MakeSigner(p.bc.chainConfig, block.Number(), block.Time())
	)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}

	// var txReqs []*ParallelTxRequest
	for i, tx := range block.Transactions() {
		// can be moved it into slot for efficiency, but signer is not concurrent safe
		// Parallel Execution 1.0&2.0 is for full sync mode, Nonce PreCheck is not necessary
		// And since we will do out-of-order execution, the Nonce PreCheck could fail.
		// We will disable it and leave it to Parallel 3.0 which is for validator mode
		msg, err := TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
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
			systemAddrRedo:  false, // set to true, when systemAddr access is detected.
			runnable:        1,     // 0: not runnable, 1: runnable
			executedNum:     0,
			retryNum:        0,
		}
		p.allTxReqs = append(p.allTxReqs, txReq)
	}
	// set up stage2 enter criteria
	p.targetStage2Count = len(p.allTxReqs)
	if p.targetStage2Count > 50 {
		// usually, the last Tx could be the bottleneck it could be very slow,
		// so it is better for us to enter stage 2 a bit earlier
		p.targetStage2Count = p.targetStage2Count - stage2AheadNum
	}

	p.doStaticDispatch(p.allTxReqs) // todo: put txReqs in unit?

	// after static dispatch, we notify the slot to work.
	for _, slot := range p.slotState {
		slot.primaryWakeUpChan <- struct{}{}
	}

	// wait until all Txs have processed.
	for {
		if len(commonTxs) == txNum {
			// put it ahead of chan receive to avoid waiting for empty block
			break
		}
		unconfirmedResult := <-p.txResultChan
		unconfirmedTxIndex := unconfirmedResult.txReq.txIndex
		if unconfirmedTxIndex <= p.mergedTxIndex {
			// log.Warn("drop merged txReq", "unconfirmedTxIndex", unconfirmedTxIndex, "p.mergedTxIndex", p.mergedTxIndex)
			continue
		}
		p.pendingConfirmResults[unconfirmedTxIndex] = append(p.pendingConfirmResults[unconfirmedTxIndex], unconfirmedResult)

		// schedule prefetch once only when unconfirmedResult is valid
		if unconfirmedResult.err == nil {
			if _, ok := p.txReqExecuteRecord[unconfirmedTxIndex]; !ok {
				p.txReqExecuteRecord[unconfirmedTxIndex] = 0
				p.txReqExecuteCount++
				statedb.AddrPrefetch(unconfirmedResult.slotDB) // todo: prefetch when it is not merged
				// enter stage2, RT confirm
				if !p.inConfirmStage2 && p.txReqExecuteCount == p.targetStage2Count {
					p.inConfirmStage2 = true
				}
			}
			p.txReqExecuteRecord[unconfirmedTxIndex]++
		}

		for {
			result := p.confirmTxResults(statedb, gp)
			if result == nil {
				break
			}
			// update tx result
			if result.err != nil {
				log.Error("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
					"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
				p.doCleanUp()
				return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", result.txReq.txIndex, result.txReq.tx.Hash().Hex(), result.err)
			}
			commonTxs = append(commonTxs, result.txReq.tx)
			receipts = append(receipts, result.receipt)
		}
	}

	// to do clean up when the block is processed
	p.doCleanUp()

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	if len(commonTxs) > 0 {
		log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
			"txNum", txNum,
			"len(commonTxs)", len(commonTxs),
			"conflictNum", p.debugConflictRedoNum,
			"redoRate(%)", 100*(p.debugConflictRedoNum)/len(commonTxs))
	}

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Number(), block.Time()) {
		return nil, nil, 0, errors.New("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, commonTxs, block.Uncles(), withdrawals)

	var allLogs []*types.Log
	for _, receipt := range receipts {
		allLogs = append(allLogs, receipt.Logs...)
	}
	return receipts, allLogs, *usedGas, nil
}

func applyTransactionStageExecution(msg *Message, gp *GasPool, statedb *state.ParallelStateDB, evm *vm.EVM) (*vm.EVM, *ExecutionResult, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(evm, msg, gp)

	if err != nil {
		return nil, nil, err
	}

	return evm, result, err
}

func applyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg Message, config *params.ChainConfig,
	statedb *state.ParallelStateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, nonce *uint64) (*types.Receipt, error) {

	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
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
	receipt.Logs = statedb.GetLogs(tx.Hash(), header.Number.Uint64(), header.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = header.Hash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, nil
}
