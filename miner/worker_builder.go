package miner

import (
	"errors"
	mapset "github.com/deckarep/golang-set/v2"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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
func (w *worker) fillTransactionsAndBundles(interrupt *atomic.Int32, env *environment) error {
	// TODO will remove after fix txpool perf issue
	if interrupt != nil {
		if signal := interrupt.Load(); signal != commitInterruptNone {
			return signalToErr(signal)
		}
	}

	bundles := w.eth.TxPool().PendingBundles(env.header.Number.Uint64(), env.header.Time)

	// if no bundles, not necessary to fill transactions
	if len(bundles) == 0 {
		return errors.New("no bundles in bundle pool")
	}

	txs, _, err := w.generateOrderedBundles(env, bundles)
	if err != nil {
		log.Error("fail to generate ordered bundles", "err", err)
		return err
	}

	if err = w.commitBundles(env, txs, interrupt); err != nil {
		log.Error("fail to commit bundles", "err", err)
		// if commit bundles failed, set profit is 0, discard all fill bundles
		env.profit.Set(big.NewInt(0))
		return err
	}
	log.Info("fill bundles", "bundles_count", len(bundles))

	start := time.Now()
	pending := w.eth.TxPool().Pending(true)
	packFromTxpoolTimer.UpdateSince(start)
	log.Debug("packFromTxpoolTimer", "duration", common.PrettyDuration(time.Since(start)), "hash", env.header.Hash())

	// Split the pending transactions into locals and remotes.
	localTxs, remoteTxs := make(map[common.Address][]*txpool.LazyTransaction), pending

	// Fill the block with all available pending transactions.
	start = time.Now()
	if len(localTxs) > 0 {
		txs := newTransactionsByPriceAndNonce(env.signer, localTxs, env.header.BaseFee)
		if err := w.commitTransactions(env, txs, interrupt); err != nil {
			return err
		}
	}
	if len(remoteTxs) > 0 {
		txs := newTransactionsByPriceAndNonce(env.signer, remoteTxs, env.header.BaseFee)
		if err := w.commitTransactions(env, txs, interrupt); err != nil {
			return err
		}
	}
	commitTxpoolTxsTimer.UpdateSince(start)
	log.Debug("commitTxpoolTxsTimer", "duration", common.PrettyDuration(time.Since(start)), "hash", env.header.Hash())
	log.Info("fill bundles and transactions done", "total_txs_count", len(env.txs))
	return nil
}

func (w *worker) commitBundles(
	env *environment,
	txs types.Transactions,
	interrupt *atomic.Int32,
) error {
	gasLimit := env.header.GasLimit
	if env.gasPool == nil {
		env.gasPool = new(core.GasPool).AddGas(gasLimit)
	}
	var coalescedLogs []*types.Log

	for _, tx := range txs {
		if interrupt != nil {
			if signal := interrupt.Load(); signal != commitInterruptNone {
				return signalToErr(signal)
			}
		}
		// If we don't have enough gas for any further transactions then we're done.
		if env.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", env.gasPool, "want", params.TxGas)
			break
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

		logs, err := w.commitBundleTransaction(env, tx, env.UnRevertible.Contains(tx.Hash()))
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
	env *environment,
	bundles []*types.Bundle,
) (types.Transactions, *types.SimulatedBundle, error) {
	// sort bundles according to gas price computed when received
	sort.SliceStable(bundles, func(i, j int) bool {
		priceI, priceJ := bundles[i].Price, bundles[j].Price

		return priceI.Cmp(priceJ) >= 0
	})

	// recompute bundle gas price based on the same state and current env
	simulatedBundles, err := w.simulateBundles(env, bundles)
	if err != nil {
		log.Error("fail to simulate bundles base on the same state", "err", err)
		return nil, nil, err
	}

	// sort bundles according to fresh gas price
	sort.SliceStable(simulatedBundles, func(i, j int) bool {
		priceI, priceJ := simulatedBundles[i].BundleGasPrice, simulatedBundles[j].BundleGasPrice

		return priceI.Cmp(priceJ) >= 0
	})

	// merge bundles based on iterative state
	includedTxs, mergedBundle, err := w.mergeBundles(env, simulatedBundles)
	if err != nil {
		log.Error("fail to merge bundles", "err", err)
		return nil, nil, err
	}

	return includedTxs, mergedBundle, nil
}

func (w *worker) simulateBundles(env *environment, bundles []*types.Bundle) ([]*types.SimulatedBundle, error) {
	headerHash := env.header.Hash()
	simCache := w.bundleCache.GetBundleCache(headerHash)
	simResult := make(map[common.Hash]*types.SimulatedBundle)

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, bundle := range bundles {
		if simmed, ok := simCache.GetSimulatedBundle(bundle.Hash()); ok {
			mu.Lock()
			simResult[bundle.Hash()] = simmed
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(idx int, bundle *types.Bundle, state *state.StateDB) {
			defer wg.Done()

			simmed, err := w.simulateBundle(env, bundle, state, env.gasPool, 0, true, true)
			if err != nil {
				log.Trace("Error computing gas for a simulateBundle", "error", err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			simResult[bundle.Hash()] = simmed
		}(i, bundle, env.state.Copy())
	}

	wg.Wait()

	simulatedBundles := make([]*types.SimulatedBundle, 0)

	for _, bundle := range simResult {
		if bundle == nil {
			continue
		}

		simulatedBundles = append(simulatedBundles, bundle)
	}

	simCache.UpdateSimulatedBundles(simResult, bundles)

	return simulatedBundles, nil
}

// mergeBundles merges the given simulateBundle into the given environment.
// It returns the merged simulateBundle and the number of transactions that were merged.
func (w *worker) mergeBundles(
	env *environment,
	bundles []*types.SimulatedBundle,
) (types.Transactions, *types.SimulatedBundle, error) {
	currentState := env.state.Copy()
	gasPool := prepareGasPool(env.header.GasLimit)
	env.UnRevertible = mapset.NewSet[common.Hash]()

	includedTxs := types.Transactions{}
	mergedBundle := types.SimulatedBundle{
		BundleGasFees:  new(big.Int),
		BundleGasUsed:  0,
		BundleGasPrice: new(big.Int),
	}

	for _, bundle := range bundles {
		prevState := currentState.Copy()
		prevGasPool := new(core.GasPool).AddGas(gasPool.Gas())

		// the floor gas price is 99/100 what was simulated at the top of the block
		floorGasPrice := new(big.Int).Mul(bundle.BundleGasPrice, big.NewInt(99))
		floorGasPrice = floorGasPrice.Div(floorGasPrice, big.NewInt(100))

		simulatedBundle, err := w.simulateBundle(env, bundle.OriginalBundle, currentState, gasPool, len(includedTxs), true, false)

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

// simulateBundle computes the gas price for a whole simulateBundle based on the same ctx
// named computeBundleGas in flashbots
func (w *worker) simulateBundle(
	env *environment, bundle *types.Bundle, state *state.StateDB, gasPool *core.GasPool, currentTxCount int,
	prune, pruneGasExceed bool,
) (*types.SimulatedBundle, error) {
	var (
		tempGasUsed   uint64
		bundleGasUsed uint64
		bundleGasFees = new(big.Int)
	)

	for i, tx := range bundle.Txs {
		state.SetTxContext(tx.Hash(), i+currentTxCount)

		receipt, err := core.ApplyTransaction(w.chainConfig, w.chain, &w.coinbase, gasPool, state, env.header, tx,
			&tempGasUsed, *w.chain.GetVMConfig())
		if err != nil {
			log.Warn("fail to simulate bundle", "hash", bundle.Hash().String(), "err", err)

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

			if prune {
				w.eth.TxPool().PruneBundle(bundle.Hash())
				log.Warn("prune bundle", "hash", bundle.Hash().String())
			}

			return nil, err
		}

		bundleGasUsed += receipt.GasUsed

		txGasUsed := new(big.Int).SetUint64(receipt.GasUsed)
		txGasFees := new(big.Int).Mul(txGasUsed, tx.GasPrice())
		bundleGasFees.Add(bundleGasFees, txGasFees)
	}

	bundleGasPrice := new(big.Int).Div(bundleGasFees, new(big.Int).SetUint64(bundleGasUsed))

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

func containsHash(arr []common.Hash, match common.Hash) bool {
	for _, elem := range arr {
		if elem == match {
			return true
		}
	}
	return false
}

func prepareGasPool(gasLimit uint64) *core.GasPool {
	gasPool := new(core.GasPool).AddGas(gasLimit)
	gasPool.SubGas(params.SystemTxsGas)
	return gasPool
}
