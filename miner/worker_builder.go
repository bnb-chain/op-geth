package miner

import (
	"context"
	"errors"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

var (
	errNonRevertingTxInBundleFailed = errors.New("non-reverting tx in bundle failed")
	errBundlePriceTooLow            = errors.New("bundle price too low")
)

// fillTransactions retrieves the pending bundles and transactions from the txpool and fills them
// into the given sealing block. The selection and ordering strategy can be extended in the future.
func (w *worker) fillTransactionsAndBundles(ctx context.Context, interrupt *atomic.Int32, env *environment) error {
	// TODO will remove after fix txpool perf issue
	if interrupt != nil {
		if signal := interrupt.Load(); signal != commitInterruptNone {
			log.Warn("fill bundles interrupted by signal")
			return errFillBundleInterrupted
		}
	}

	fillBundlesStart := time.Now()

	pendingStart := time.Now()
	bundles := w.eth.TxPool().PendingBundles(env.header.Number.Uint64(), env.header.Time)
	pendingDuration := time.Since(pendingStart)

	// if no bundles, not necessary to fill transactions
	if len(bundles) == 0 {
		log.Warn("no bundles in bundle pool")
		return errFillBundleInterrupted
	}

	genStart := time.Now()
	txs, _, err := w.generateOrderedBundles(ctx, env, bundles)
	genDuration := time.Since(genStart)
	if err != nil {
		log.Error("fail to generate ordered bundles", "err", err, "elapsed", common.PrettyDuration(genDuration))
		return errFillBundleInterrupted
	}

	bundleHashes := make([]common.Hash, len(bundles))
	for i, b := range bundles {
		bundleHashes[i] = b.Hash()
	}

	commitStart := time.Now()
	if err = w.commitBundles(ctx, env, txs, interrupt); err != nil {
		log.Error("Failed to commit bundles", "err", err, "bundleCount", len(bundles), "txCount", len(txs),
			"bundleHashes", bundleHashes,
			"pendingElapsed", common.PrettyDuration(pendingDuration),
			"generateElapsed", common.PrettyDuration(genDuration),
			"commitElapsed", common.PrettyDuration(time.Since(commitStart)),
			"totalElapsed", common.PrettyDuration(time.Since(fillBundlesStart)))
		return errFillBundleInterrupted
	}
	log.Info("Filled bundles", "bundles_count", len(bundles), "txCount", len(txs),
		"bundleHashes", bundleHashes,
		"pendingElapsed", common.PrettyDuration(pendingDuration),
		"generateElapsed", common.PrettyDuration(genDuration),
		"commitElapsed", common.PrettyDuration(time.Since(commitStart)),
		"totalElapsed", common.PrettyDuration(time.Since(fillBundlesStart)))

	w.mu.RLock()
	tip := w.tip
	w.mu.RUnlock()

	start := time.Now()
	// Retrieve the pending transactions pre-filtered by the 1559/4844 dynamic fees
	filter := txpool.PendingFilter{
		MinTip: tip,
	}
	if env.header.BaseFee != nil {
		filter.BaseFee = uint256.MustFromBig(env.header.BaseFee)
	}
	if env.header.ExcessBlobGas != nil {
		filter.BlobFee = uint256.MustFromBig(eip4844.CalcBlobFee(*env.header.ExcessBlobGas))
	}
	filter.OnlyPlainTxs, filter.OnlyBlobTxs = true, false
	pendingPlainTxs := w.eth.TxPool().Pending(filter)

	filter.OnlyPlainTxs, filter.OnlyBlobTxs = false, true
	pendingBlobTxs := w.eth.TxPool().Pending(filter)

	packFromTxpoolTimer.UpdateSince(start)
	log.Debug("packFromTxpoolTimer", "duration", common.PrettyDuration(time.Since(start)), "hash", env.header.Hash(), "txs", len(pendingPlainTxs))

	// Split the pending transactions into locals and remotes.
	localPlainTxs, remotePlainTxs := make(map[common.Address][]*txpool.LazyTransaction), pendingPlainTxs
	localBlobTxs, remoteBlobTxs := make(map[common.Address][]*txpool.LazyTransaction), pendingBlobTxs

	for _, account := range w.eth.TxPool().Locals() {
		if txs := remotePlainTxs[account]; len(txs) > 0 {
			delete(remotePlainTxs, account)
			localPlainTxs[account] = txs
		}
		if txs := remoteBlobTxs[account]; len(txs) > 0 {
			delete(remoteBlobTxs, account)
			localBlobTxs[account] = txs
		}
	}
	// Fill the block with all available pending transactions.
	start = time.Now()
	if len(localPlainTxs) > 0 || len(localBlobTxs) > 0 {
		plainTxs := newTransactionsByPriceAndNonce(env.signer, localPlainTxs, env.header.BaseFee)
		blobTxs := newTransactionsByPriceAndNonce(env.signer, localBlobTxs, env.header.BaseFee)

		if err := w.commitTransactions(env, plainTxs, blobTxs, interrupt); err != nil {
			return err
		}
	}
	if len(remotePlainTxs) > 0 || len(remoteBlobTxs) > 0 {
		plainTxs := newTransactionsByPriceAndNonce(env.signer, remotePlainTxs, env.header.BaseFee)
		blobTxs := newTransactionsByPriceAndNonce(env.signer, remoteBlobTxs, env.header.BaseFee)

		if err := w.commitTransactions(env, plainTxs, blobTxs, interrupt); err != nil {
			return err
		}
	}
	commitTxpoolTxsTimer.UpdateSince(start)
	log.Debug("commitTxpoolTxsTimer", "duration", common.PrettyDuration(time.Since(start)), "hash", env.header.Hash())
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Info("fill bundles and transactions done", "total_txs_count", len(env.txs))
	return nil
}

func (w *worker) commitBundles(
	ctx context.Context,
	env *environment,
	txs types.Transactions,
	interrupt *atomic.Int32,
) error {
	gasLimit := prepareGasPool()
	if env.gasPool == nil {
		env.gasPool = new(core.GasPool).AddGas(gasLimit.Gas())
	}
	var coalescedLogs []*types.Log

	for _, tx := range txs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if interrupt != nil {
			if signal := interrupt.Load(); signal != commitInterruptNone {
				return errors.New("failed bundle commit due to payload timeout or resolve")
			}
		}
		// If we don't have enough gas for any further transactions then we're done.
		if env.gasPool.Gas() < params.TxGas {
			return errors.New("not enough gas for further transactions")
		}
		if tx == nil {
			return errors.New("unexpected nil transaction in bundle")
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(env.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.chainConfig.IsEIP155(env.header.Number) {
			return errors.New("unexpected protected transaction in bundle")
		}
		// Start executing the transaction
		env.state.SetTxContext(tx.Hash(), env.tcount)

		logs, err := w.commitBundleTransaction(ctx, env, tx, env.UnRevertible.Contains(tx.Hash()))
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Error("Unexpected gas limit exceeded for current block in the bundle", "sender", from)
			return signalToErr(commitInterruptBundleCommit)

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Error("Transaction with low nonce in the bundle", "sender", from, "nonce", tx.Nonce())
			return signalToErr(commitInterruptBundleCommit)

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Error("Account with high nonce in the bundle", "sender", from, "nonce", tx.Nonce())
			return signalToErr(commitInterruptBundleCommit)

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			env.tcount++
			continue

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Error("Transaction failed in the bundle", "hash", tx.Hash(), "err", err)
			return signalToErr(commitInterruptBundleCommit)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the worker will regenerate a mining block every second.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		w.pendingLogsFeed.Send(cpy)
	}
	return nil
}

// generateOrderedBundles generates ordered txs from the given bundles.
// 1. sort bundles according to computed gas price when received.
// 2. simulate bundles based on the same state, resort.
// 3. merge resorted simulateBundles based on the iterative state.
func (w *worker) generateOrderedBundles(
	ctx context.Context,
	env *environment,
	bundles []*types.Bundle,
) (types.Transactions, *types.SimulatedBundle, error) {
	// recompute bundle gas price based on the same state and current env
	simulatedBundles, err := w.simulateBundles(ctx, env, bundles)
	if err != nil {
		log.Error("fail to simulate bundles base on the same state", "err", err)
		return nil, nil, err
	}

	// sort bundles according to fresh gas price
	slices.SortStableFunc(simulatedBundles, func(i, j *types.SimulatedBundle) int {
		priceI, priceJ := i.BundleGasPrice, j.BundleGasPrice
		return priceJ.Cmp(priceI)
	})

	// merge bundles based on iterative state
	includedTxs, mergedBundle, err := w.mergeBundles(ctx, env, simulatedBundles)
	if err != nil {
		log.Error("fail to merge bundles", "err", err)
		return nil, nil, err
	}

	return includedTxs, mergedBundle, nil
}

// simulateBundles starts a fixed number of workers. A worker owns execution
// capacity before it copies state; copies from the shared source are serialized.
func (w *worker) simulateBundles(ctx context.Context, env *environment, bundles []*types.Bundle) ([]*types.SimulatedBundle, error) {
	selected, err := selectBuilderBundles(ctx, bundles)
	if err != nil {
		return nil, err
	}
	simCache := w.bundleCache.GetBundleCache(env.header.Hash())
	results := make([]*types.SimulatedBundle, len(selected))
	var next int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < min(bundleBuilderWorkers, len(selected)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if ctx.Err() != nil || next == len(selected) {
					mu.Unlock()
					return
				}
				idx := next
				next++
				bundle := selected[idx]
				if cached, ok := simCache.GetSimulatedBundle(bundle.Hash()); ok {
					results[idx] = cached
					mu.Unlock()
					continue
				}
				simulationState := env.state.Copy()
				mu.Unlock()
				if ctx.Err() != nil {
					return
				}
				simmed, err := w.simulateBundleWithContext(ctx, env, bundle, simulationState, prepareGasPool(), 0, true, true, nil)
				// Interrupted/queued work must never become a negative cache entry.
				if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				simCache.results.Add(bundle.Hash(), simmed)
				results[idx] = simmed
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	simulated := make([]*types.SimulatedBundle, 0, len(results))
	for _, bundle := range results {
		if bundle != nil {
			simulated = append(simulated, bundle)
		}
	}
	return simulated, nil
}

// mergeBundles merges the given simulateBundle into the given environment.
// It returns the merged simulateBundle and the number of transactions that were merged.
func (w *worker) mergeBundles(
	ctx context.Context,
	env *environment,
	bundles []*types.SimulatedBundle,
) (types.Transactions, *types.SimulatedBundle, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	currentState := env.state.Copy()
	gasPool := prepareGasPool()
	env.UnRevertible = mapset.NewSet[common.Hash]()

	includedTxs := types.Transactions{}
	mergedBundle := types.SimulatedBundle{
		BundleGasFees:  new(big.Int),
		BundleGasUsed:  0,
		BundleGasPrice: new(big.Int),
	}

	for _, bundle := range bundles {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		prevState := currentState.Copy()
		prevGasPool := new(core.GasPool).AddGas(gasPool.Gas())

		// the floor gas price is 99/100 what was simulated at the top of the block
		floorGasPrice := new(big.Int).Mul(bundle.BundleGasPrice, big.NewInt(99))
		floorGasPrice = floorGasPrice.Div(floorGasPrice, big.NewInt(100))

		// Conflicts on tentative merged state do not invalidate a bundle at the
		// parent. The entire merge may still be discarded on cancellation.
		simulatedBundle, err := w.simulateBundleWithContext(ctx, env, bundle.OriginalBundle, currentState, gasPool, len(includedTxs), false, false, nil)
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		if err != nil && errors.Is(err, core.ErrGasLimitReached) {
			log.Error("failed to merge bundle, interrupt merge process", "err", err)
			break
		}
		if err != nil || simulatedBundle.BundleGasPrice.Cmp(floorGasPrice) <= 0 {
			currentState = prevState
			gasPool = prevGasPool

			log.Error("failed to merge bundle", "floorGasPrice", floorGasPrice, "err", err)
			continue
		}

		log.Info("included bundle",
			"gasUsed", simulatedBundle.BundleGasUsed,
			"gasPrice", simulatedBundle.BundleGasPrice,
			"txcount", len(simulatedBundle.OriginalBundle.Txs))

		includedTxs = append(includedTxs, bundle.OriginalBundle.Txs...)

		mergedBundle.BundleGasFees.Add(mergedBundle.BundleGasFees, simulatedBundle.BundleGasFees)
		mergedBundle.BundleGasUsed += simulatedBundle.BundleGasUsed

		for _, tx := range includedTxs {
			if !containsHash(bundle.OriginalBundle.RevertingTxHashes, tx.Hash()) {
				env.UnRevertible.Add(tx.Hash())
			}
		}
	}

	if len(includedTxs) == 0 {
		return nil, nil, errors.New("include no txs when merge bundles")
	}

	mergedBundle.BundleGasPrice.Div(mergedBundle.BundleGasFees, new(big.Int).SetUint64(mergedBundle.BundleGasUsed))

	return includedTxs, &mergedBundle, nil
}

// simulateBundleWithContext computes bundle gas price on disposable state.
func (w *worker) simulateBundleWithContext(
	ctx context.Context, env *environment, bundle *types.Bundle, state *state.StateDB, gasPool *core.GasPool, currentTxCount int,
	prune, pruneGasExceed bool, publicTxs map[common.Hash]bool,
) (*types.SimulatedBundle, error) {
	var (
		tempGasUsed   uint64
		bundleGasUsed uint64
		bundleGasFees = new(big.Int)
	)

	for i, tx := range bundle.Txs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		state.SetTxContext(tx.Hash(), i+currentTxCount)

		receipt, err := core.ApplyTransactionWithContext(ctx, w.chainConfig, w.chain, &w.coinbase, gasPool, state, env.header, tx,
			&tempGasUsed, *w.chain.GetVMConfig())
		if err != nil {
			log.Warn("fail to simulate bundle", "hash", bundle.Hash().String(), "err", err)

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if prune {
				if errors.Is(err, core.ErrGasLimitReached) && !pruneGasExceed {
					log.Warn("bundle gas limit exceed", "hash", bundle.Hash().String())
				} else {
					log.Warn("prune bundle", "hash", bundle.Hash().String(), "err", err)
					w.eth.TxPool().PruneBundle(bundle.Hash())
				}
			}

			return nil, err
		}

		if receipt.Status == types.ReceiptStatusFailed && !containsHash(bundle.RevertingTxHashes, receipt.TxHash) {
			err = errNonRevertingTxInBundleFailed
			log.Warn("fail to simulate bundle", "hash", bundle.Hash().String(), "err", err)

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if prune {
				w.eth.TxPool().PruneBundle(bundle.Hash())
				log.Warn("prune bundle", "hash", bundle.Hash().String())
			}

			return nil, err
		}
		known := publicTxs[tx.Hash()]
		if publicTxs == nil {
			known = w.eth.TxPool().Has(tx.Hash())
		}
		if !known {
			bundleGasUsed += receipt.GasUsed

			txGasUsed := new(big.Int).SetUint64(receipt.GasUsed)
			effectiveTip, er := tx.EffectiveGasTip(env.header.BaseFee)
			if er != nil {
				return nil, er
			}
			if env.header.BaseFee != nil {
				effectiveTip.Add(effectiveTip, env.header.BaseFee)
			}
			txGasFees := new(big.Int).Mul(txGasUsed, effectiveTip)
			bundleGasFees.Add(bundleGasFees, txGasFees)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Transactions already in the ordinary txpool do not contribute to bundle price.
	bundleGasPrice := big.NewInt(0)
	if bundleGasUsed != 0 {
		bundleGasPrice = new(big.Int).Div(bundleGasFees, new(big.Int).SetUint64(bundleGasUsed))
	}

	if bundleGasPrice.Cmp(big.NewInt(w.config.Mev.MevBundleGasPriceFloor)) < 0 {
		err := errBundlePriceTooLow
		log.Warn("fail to simulate bundle", "hash", bundle.Hash().String(), "err", err)

		if prune {
			log.Warn("prune bundle", "hash", bundle.Hash().String())
			w.eth.TxPool().PruneBundle(bundle.Hash())
		}

		return nil, err
	}

	return &types.SimulatedBundle{
		OriginalBundle: bundle,
		BundleGasFees:  bundleGasFees,
		BundleGasPrice: bundleGasPrice,
		BundleGasUsed:  bundleGasUsed,
	}, nil
}

func (w *worker) simulateGaslessBundle(ctx context.Context, env *environment, bundle *types.Bundle) (*types.SimulateGaslessBundleResp, error) {
	validResults := make([]types.GaslessTxSimResult, 0)
	gasReachedResults := make([]types.GaslessTxSimResult, 0)

	txIdx := 0
	for _, tx := range bundle.Txs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		env.state.SetTxContext(tx.Hash(), txIdx)

		var (
			snap = env.state.Snapshot()
			gp   = env.gasPool.Gas()
		)

		receipt, err := core.ApplyTransactionWithContext(ctx, w.chainConfig, w.chain, &w.coinbase, env.gasPool, env.state, env.header, tx,
			&env.header.GasUsed, *w.chain.GetVMConfig())
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			env.state.RevertToSnapshot(snap)
			env.gasPool.SetGas(gp)
			log.Error("fail to simulate gasless bundle, skipped", "txHash", tx.Hash(), "err", err)
			if err == core.ErrGasLimitReached {
				gasReachedResults = append(gasReachedResults, types.GaslessTxSimResult{Hash: tx.Hash()})
			}
		} else {
			txIdx++
			validResults = append(validResults, types.GaslessTxSimResult{
				Hash:    tx.Hash(),
				GasUsed: receipt.GasUsed,
			})
		}
	}

	return &types.SimulateGaslessBundleResp{
		ValidResults:      validResults,
		GasReachedResults: gasReachedResults,
		BasedBlockNumber:  env.header.Number.Int64(),
	}, nil
}

func containsHash(arr []common.Hash, match common.Hash) bool {
	for _, elem := range arr {
		if elem == match {
			return true
		}
	}
	return false
}

func prepareGasPool() *core.GasPool {
	gasPool := new(core.GasPool).AddGas(params.BundleGasLimit)
	return gasPool
}
