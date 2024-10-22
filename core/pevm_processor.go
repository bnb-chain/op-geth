package core

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
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
}

func newPEVMProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *PEVMProcessor {
	processor := &PEVMProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		unorderedMerge: bc.vmConfig.EnableParallelUnorderedMerge,
	}
	log.Info("Parallel execution mode is enabled", "Parallel Num", ParallelNum(),
		"CPUNum", runtime.NumCPU(), "unorderedMerge", processor.unorderedMerge)
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
	baseStateDB *state.StateDB
	tx          *types.Transaction
	gasLimit    uint64
	msg         *Message
	block       *types.Block
	vmConfig    vm.Config
	usedGas     *uint64
	useDAG      bool
	value       int // only for unit test
}

// resetState clear slot state for each block.
func (p *PEVMProcessor) resetState(txNum int, statedb *state.StateDB) {
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
		log.Debug("executed result conflicts", "err", err)
		return err
	}
	return nil
}

// executeInSlot do tx execution with thread local slot.
func (p *PEVMProcessor) executeInSlot(maindb *state.StateDB, txReq *PEVMTxRequest) *PEVMTxResult {
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
func (p *PEVMProcessor) toConfirmTxIndexResult(txResult *PEVMTxResult) error {
	txReq := txResult.txReq
	if !p.unorderedMerge || !txReq.useDAG {
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
func (p *PEVMProcessor) confirmTxResult(statedb *state.StateDB, gp *GasPool, result *PEVMTxResult) error {
	checkErr := p.toConfirmTxIndexResult(result)
	// ok, the tx result is valid and can be merged
	if checkErr != nil {
		return checkErr
	}

	if err := gp.SubGas(result.receipt.GasUsed); err != nil {
		log.Error("gas limit reached", "block", result.txReq.block.Number(),
			"txIndex", result.txReq.txIndex, "GasUsed", result.receipt.GasUsed, "gp.Gas", gp.Gas())
		return fmt.Errorf("gas limit reached")
	}

	var root []byte
	header := result.txReq.block.Header()

	isByzantium := p.config.IsByzantium(header.Number)
	isEIP158 := p.config.IsEIP158(header.Number)
	//result.slotDB.FinaliseForParallel(isByzantium || isEIP158, statedb)
	if err := result.slotDB.Merge(isByzantium || isEIP158); err != nil {
		// something very wrong, should not happen
		log.Error("merge slotDB failed", "err", err)
		return err
	}
	result.slotDB.Finalise(isByzantium || isEIP158)

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
	p.receipts[result.txReq.txIndex] = result.receipt
	p.commonTxs[result.txReq.txIndex] = result.txReq.tx
	return nil
}

// Process implements BEP-130 Parallel Transaction Execution
func (p *PEVMProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
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
			usedGas:     usedGas,
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
	var executeDurations, confirmDurations int64 = 0, 0
	err, txIndex := txLevels.Run(func(pr *PEVMTxRequest) (res *PEVMTxResult) {
		defer func(t0 time.Time) {
			atomic.AddInt64(&executeDurations, time.Since(t0).Nanoseconds())
			if res.err != nil {
				atomic.AddUint64(&p.debugConflictRedoNum, 1)
			}
		}(time.Now())

		if err := buildMessage(pr, signer, header); err != nil {
			return &PEVMTxResult{txReq: pr, err: err}
		}
		return p.executeInSlot(statedb, pr)
	}, func(pr *PEVMTxResult) (err error) {
		defer func(t0 time.Time) {
			atomic.AddInt64(&confirmDurations, time.Since(t0).Nanoseconds())
			if err != nil {
				atomic.AddUint64(&p.debugConflictRedoNum, 1)
			}
		}(time.Now())
		log.Debug("pevm confirm", "txIndex", pr.txReq.txIndex)
		return p.confirmTxResult(statedb, gp, pr)
	}, p.unorderedMerge && txDAG != nil)
	parallelRunDuration := time.Since(start) - buildLevelsDuration
	if err != nil {
		tx := allTxs[txIndex]
		log.Error("ProcessParallel tx failed", "txIndex", txIndex, "txHash", tx.Hash().Hex(), "err", err)
		return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", txIndex, tx.Hash().Hex(), err)
	}

	//fmt.Printf("ProcessParallel tx all done, parallelNum:%d, txNum: %d, conflictNum: %d, executeDuration:%s, confirmDurations:%s, buildLevelsDuration:%s, runDuration:%s\n",
	//	ParallelNum(), txNum, p.debugConflictRedoNum, time.Duration(executeDurations), time.Duration(confirmDurations), buildLevelsDuration, parallelRunDuration)

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	var redoRate int = 0
	if len(p.commonTxs) == 0 {
		redoRate = 100 * (int(p.debugConflictRedoNum)) / 1
	} else {
		redoRate = 100 * (int(p.debugConflictRedoNum)) / len(p.commonTxs)
	}
	pevmBuildLevelsTimer.Update(buildLevelsDuration)
	pevmRunTimer.Update(parallelRunDuration)
	log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
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
		for _, log := range receipt.Logs {
			log.Index = uint(lindex)
			lindex++
		}
		// re-calculate the cumulativeGasUsed
		cumulativeGasUsed += receipt.GasUsed
		receipt.CumulativeGasUsed = cumulativeGasUsed
		allLogs = append(allLogs, receipt.Logs...)
	}
	return p.receipts, allLogs, *usedGas, nil
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

func pevmApplyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg Message, config *params.ChainConfig, statedb *state.UncommittedDB, block *types.Block, tx *types.Transaction, usedGas *uint64, nonce *uint64) (*types.Receipt, error) {
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
	receipt.Logs = statedb.PackLogs(block.NumberU64(), block.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = block.Hash()
	receipt.BlockNumber = block.Number()
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, nil
}
