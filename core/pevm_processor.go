package core

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
)

type PEVMProcessor struct {
	StateProcessor
	allTxReqs            []*PEVMTxRequest
	delayGasFee          bool
	commonTxs            []*types.Transaction
	receipts             types.Receipts
	debugConflictRedoNum uint64
	unorderedMerge       bool
	parallelMerge        bool
}

func newPEVMProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *PEVMProcessor {
	processor := &PEVMProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		unorderedMerge: bc.vmConfig.EnableParallelUnorderedMerge,
		parallelMerge:  bc.vmConfig.EnableTxParallelMerge,
	}
	initParallelRunner(bc.vmConfig.ParallelTxNum)
	if bc.vmConfig.ParallelThreshold == 0 {
		bc.vmConfig.ParallelThreshold = ParallelNum()
	}
	log.Info("Parallel execution mode is enabled", "Parallel Num", ParallelNum(),
		"CPUNum", runtime.GOMAXPROCS(0), "unorderedMerge", processor.unorderedMerge,
		"parallel threshold", bc.vmConfig.ParallelThreshold)
	return processor
}

type PEVMTxResult struct {
	txReq         *PEVMTxRequest
	receipt       *types.Receipt
	slotDB        *state.UncommittedDB
	gpSlot        *GasPool
	evm           *vm.EVM
	result        *ExecutionResult
	originalNonce *uint64
	err           error
}

type PEVMTxRequest struct {
	txIndex     int
	baseStateDB state.StateDBer
	tx          *types.Transaction
	gasLimit    uint64
	msg         *Message
	block       *types.Block
	vmConfig    vm.Config
	usedGas     *atomic.Uint64
	useDAG      bool
	value       int // only for unit test
}

// resetState clear slot state for each block.
func (p *PEVMProcessor) resetState(txNum int, statedb state.StateDBer) {
	statedb.PrepareForParallel()
	p.allTxReqs = make([]*PEVMTxRequest, txNum)
}

// hasConflict conducts conflict check
func (p *PEVMProcessor) hasConflict(txResult *PEVMTxResult) error {
	slotDB := txResult.slotDB
	if txResult.err != nil {
		log.Warn("execute result failed, HasConflict due to err", "err", txResult.err)
		return txResult.err
	}
	// check whether the slot db reads during execution are correct.
	if err := slotDB.ConflictsToMaindb(); err != nil {
		atomic.AddUint64(&p.debugConflictRedoNum, 1)
		log.Debug("executed result conflicts", "err", err)
		return err
	}
	return nil
}

// executeInSlot do tx execution with thread local slot.
func (p *PEVMProcessor) executeInSlot(maindb state.StateDBer, txReq *PEVMTxRequest) *PEVMTxResult {
	slotDB := state.NewUncommittedDB(maindb)
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
	evm, result, err := pevmApplyTransactionStageExecution(txReq.msg, gpSlot, slotDB, vmenv, p.delayGasFee)
	txResult := PEVMTxResult{
		txReq:         txReq,
		receipt:       nil,
		slotDB:        slotDB,
		err:           err,
		gpSlot:        gpSlot,
		evm:           evm,
		result:        result,
		originalNonce: &on,
	}
	return &txResult
}

// to confirm one txResult, return true if the result is valid
// if it is in Stage 2 it is a likely result, not 100% sure
func (p *PEVMProcessor) toConfirmTxIndexResult(txResult *PEVMTxResult, enableParallelMerge bool) error {
	txReq := txResult.txReq
	if !txReq.useDAG || (!p.unorderedMerge && !enableParallelMerge) {
		// If we do not use a DAG, then we need to check for conflicts to ensure correct execution.
		// When we perform an unordered merge, we cannot conduct conflict checks
		// and can only choose to trust that the DAG is correct and that conflicts do not exist.
		if err := p.hasConflict(txResult); err != nil {
			log.Info(fmt.Sprintf("HasConflict!! block: %d, txIndex: %d\n", txResult.txReq.block.NumberU64(), txResult.txReq.txIndex))
			return err
		}
	}

	// goroutine unsafe operation will be handled from here for safety
	gasConsumed := txReq.gasLimit - txResult.gpSlot.Gas()
	if gasConsumed != txResult.result.UsedGas {
		log.Error("gasConsumed != result.UsedGas mismatch",
			"gasConsumed", gasConsumed, "result.UsedGas", txResult.result.UsedGas)
		return fmt.Errorf("gasConsumed != result.UsedGas mismatch")
	}

	// ok, time to do finalize, stage2 should not be parallel
	txResult.receipt, txResult.err = pevmApplyTransactionStageFinalization(txResult.evm, txResult.result,
		*txReq.msg, p.config, txResult.slotDB, txReq.block,
		txReq.tx, txReq.usedGas, txResult.originalNonce)
	return nil
}

// wait until the next Tx is executed and its result is merged to the main stateDB
func (p *PEVMProcessor) confirmTxResult(statedb state.StateDBer, gp *ParallelGasPool, result *PEVMTxResult, enableParallelMerge bool) error {
	checkErr := p.toConfirmTxIndexResult(result, enableParallelMerge)
	// ok, the tx result is valid and can be merged
	if checkErr != nil {
		return checkErr
	}

	if err := gp.SubGas(result.receipt.GasUsed); err != nil {
		log.Error("gas limit reached", "block", result.txReq.block.Number(),
			"txIndex", result.txReq.txIndex, "GasUsed", result.receipt.GasUsed, "gp.Gas", gp.Gas())
		return fmt.Errorf("gas limit reached")
	}

	header := result.txReq.block.Header()

	isByzantium := p.config.IsByzantium(header.Number)
	isEIP158 := p.config.IsEIP158(header.Number)
	if err := result.slotDB.Merge(isByzantium || isEIP158); err != nil {
		// something very wrong, should not happen
		log.Error("merge slotDB failed", "err", err)
		return err
	}

	if !enableParallelMerge {
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
		var root []byte
		result.slotDB.Finalise(isByzantium || isEIP158)

		// Do IntermediateRoot after mergeSlotDB.
		if !isByzantium {
			root = statedb.IntermediateRoot(isEIP158).Bytes()
		}
		result.receipt.PostState = root
	}
	p.receipts[result.txReq.txIndex] = result.receipt
	p.commonTxs[result.txReq.txIndex] = result.txReq.tx
	return nil
}

// Process implements BEP-130 Parallel Transaction Execution
func (p *PEVMProcessor) Process(block *types.Block, statedb state.StateDBer, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		usedGas = atomic.Uint64{}
		header  = block.Header()
		gp      = new(ParallelGasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork2(statedb)
	}
	if p.config.PreContractForkBlock != nil && p.config.PreContractForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyPreContractHardFork2(statedb)
	}

	misc.EnsureCreate2Deployer(p.config, block.Time(), statedb.(vm.StateDB))

	allTxs := block.Transactions()
	p.resetState(len(allTxs), statedb)

	var (
		// with parallel mode, vmenv will be created inside of slot
		blockContext = NewEVMBlockContext(block.Header(), p.bc, nil, p.config, statedb)
		vmenv        = vm.NewEVM(blockContext, vm.TxContext{}, statedb.(vm.StateDB), p.config, cfg)
		signer       = types.MakeSigner(p.bc.chainConfig, block.Number(), block.Time())
	)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		ProcessBeaconBlockRoot2(*beaconRoot, vmenv, statedb)
	}
	statedb.MarkFullProcessed()
	txDAG := cfg.TxDAG

	txNum := len(allTxs)
	// Iterate over and process the individual transactions
	p.commonTxs = make([]*types.Transaction, txNum)
	p.receipts = make([]*types.Receipt, txNum)
	p.debugConflictRedoNum = 0

	for i, tx := range allTxs {
		// parallel start, wrap an exec message, which will be dispatched to a slot
		txReq := &PEVMTxRequest{
			txIndex:     i,
			baseStateDB: statedb,
			tx:          tx,
			gasLimit:    block.GasLimit(), // gp.Gas().
			block:       block,
			vmConfig:    cfg,
			usedGas:     &usedGas,
			useDAG:      txDAG != nil,
		}
		p.allTxReqs[i] = txReq
	}
	p.delayGasFee = true
	if txDAG != nil && !txDAG.DelayGasFeeDistribution() {
		p.delayGasFee = false
	}

	// parallel execution
	start := time.Now()
	txLevels := NewTxLevels(p.allTxReqs, txDAG)
	log.Debug("txLevels size", "txLevels size", len(txLevels))
	parallelTxLevelsSizeMeter.Update(int64(len(txLevels)))
	buildLevelsDuration := time.Since(start)

	_, ok := statedb.(*state.ParallelStateDB)
	enableParallelMerge := p.parallelMerge && ok && txDAG != nil
	var executeDurations, confirmDurations int64 = 0, 0
	err, txIndex := txLevels.Run(func(pr *PEVMTxRequest) (res *PEVMTxResult) {
		defer func(t0 time.Time) {
			atomic.AddInt64(&executeDurations, time.Since(t0).Nanoseconds())
		}(time.Now())

		if err := buildMessage(pr, signer, header); err != nil {
			return &PEVMTxResult{txReq: pr, err: err}
		}
		return p.executeInSlot(statedb, pr)
	}, func(pr *PEVMTxResult) (err error) {
		defer func(t0 time.Time) {
			atomic.AddInt64(&confirmDurations, time.Since(t0).Nanoseconds())
		}(time.Now())
		log.Debug("pevm confirm", "txIndex", pr.txReq.txIndex)
		return p.confirmTxResult(statedb, gp, pr, enableParallelMerge)
	}, func(levels TxLevels, cq *confirmQueue) (err error) {
		defer func(t0 time.Time) {
			atomic.AddInt64(&confirmDurations, time.Since(t0).Nanoseconds())
		}(time.Now())
		log.Debug("after parallel confirm")
		return p.afterParallelConfirm(statedb, block.Header(), levels, cq)
	}, p.unorderedMerge, enableParallelMerge)
	parallelRunDuration := time.Since(start) - buildLevelsDuration
	if err != nil {
		tx := allTxs[txIndex]
		log.Error("ProcessParallel tx failed", "txIndex", txIndex, "txHash", tx.Hash().Hex(), "err", err)
		return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", txIndex, tx.Hash().Hex(), err)
	}

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	var redoRate int = 0
	if len(p.commonTxs) == 0 {
		redoRate = 100 * (int(p.debugConflictRedoNum)) / 1
	} else {
		redoRate = 100 * (int(p.debugConflictRedoNum)) / len(p.commonTxs)
	}
	pevmBuildLevelsTimer.Update(buildLevelsDuration)
	pevmRunTimer.Update(parallelRunDuration)
	log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", usedGas.Load(),
		"parallelNum", ParallelNum(),
		"buildLevelsDuration", buildLevelsDuration,
		"parallelRunDuration", parallelRunDuration,
		"executeDurations", time.Duration(executeDurations),
		"confirmDurations", time.Duration(confirmDurations),
		"txNum", txNum,
		"len(commonTxs)", len(p.commonTxs),
		"conflictNum", p.debugConflictRedoNum,
		"redoRate(%)", redoRate,
		"txDAG", txDAG != nil)
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
	var lindex = 0
	var cumulativeGasUsed uint64
	for _, receipt := range p.receipts {
		// reset the log index
		for _, oneLog := range receipt.Logs {
			oneLog.Index = uint(lindex)
			lindex++
		}
		// re-calculate the cumulativeGasUsed
		cumulativeGasUsed += receipt.GasUsed
		receipt.CumulativeGasUsed = cumulativeGasUsed
		allLogs = append(allLogs, receipt.Logs...)
	}
	return p.receipts, allLogs, usedGas.Load(), nil
}

func (p *PEVMProcessor) afterParallelConfirm(statedb state.StateDBer, header *types.Header, levels TxLevels, cq *confirmQueue) error {
	txCount := levels.txCount()
	tipChan := make(chan *state.DelayedGasFee, txCount)
	baseChan := make(chan *state.DelayedGasFee, txCount)
	l1Chan := make(chan *state.DelayedGasFee, txCount)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			gasFee, ok := <-tipChan
			if !ok {
				return
			}
			statedb.AddBalance(gasFee.Coinbase, gasFee.TipFee)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			gasFee, ok := <-baseChan
			if !ok {
				return
			}
			statedb.AddBalance(params.OptimismBaseFeeRecipient, gasFee.BaseFee)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			gasFee, ok := <-l1Chan
			if !ok {
				return
			}
			statedb.AddBalance(params.OptimismL1FeeRecipient, gasFee.L1Fee)
		}
	}()
	for _, txs := range levels {
		for _, tx := range txs {
			toConfirm := cq.queue[tx.txIndex]
			result := toConfirm.result
			delayGasFee := result.result.delayFees
			if delayGasFee != nil {
				if delayGasFee.TipFee != nil {
					tipChan <- delayGasFee
				}
				if delayGasFee.BaseFee != nil {
					baseChan <- delayGasFee
				}
				if delayGasFee.L1Fee != nil {
					l1Chan <- delayGasFee
				}
			}
		}
	}
	close(tipChan)
	close(baseChan)
	close(l1Chan)
	wg.Wait()

	isByzantium := p.config.IsByzantium(header.Number)
	if !isByzantium {
		panic("afterParallelConfirm not support before Byzantium block")
	}
	isEIP158 := p.config.IsEIP158(header.Number)

	statedb.Finalise(isByzantium || isEIP158)

	return nil
}

func buildMessage(txReq *PEVMTxRequest, signer types.Signer, header *types.Header) error {
	if txReq.msg != nil {
		return nil
	}
	msg, err := TransactionToMessage(txReq.tx, signer, header.BaseFee)
	if err != nil {
		return err
		//return fmt.Errorf("could not apply tx %d [%v]: %w", txReq.txIndex, txReq.tx.Hash().Hex(), err)
	}
	txReq.msg = msg
	return nil
}

func pevmApplyTransactionStageExecution(msg *Message, gp *GasPool, statedb *state.UncommittedDB, evm *vm.EVM, delayGasFee bool) (*vm.EVM, *ExecutionResult, error) {
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

func pevmApplyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg Message, config *params.ChainConfig, statedb *state.UncommittedDB, block *types.Block, tx *types.Transaction, usedGas *atomic.Uint64, nonce *uint64) (*types.Receipt, error) {
	usedGas.Add(result.UsedGas)
	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: nil, CumulativeGasUsed: usedGas.Load()}
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
	receipt.Logs = statedb.PackLogs(block.NumberU64(), block.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = block.Hash()
	receipt.BlockNumber = block.Number()
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, nil
}
