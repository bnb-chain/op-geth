package miner

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"golang.org/x/time/rate"
)

// ErrBundleSimulationBusy signals overload without queuing more state copies or
// EVMs. It is also returned for an equivalent simulation already in progress.
var ErrBundleSimulationBusy = errors.New("bundle simulation busy")

type simulationKey struct {
	hash    common.Hash
	gasless bool
}

// bundleSimulationLimiter is shared by both RPC simulation methods. Payload
// building has its own execution path and never waits for this budget.
type bundleSimulationLimiter struct {
	mu          sync.Mutex
	running     int
	concurrency int
	timeout     time.Duration
	gas         *rate.Limiter
	inflight    map[simulationKey]struct{}
}

func newBundleSimulationLimiter(config MevConfig) *bundleSimulationLimiter {
	if config.MaxConcurrentSimulations <= 0 {
		config.MaxConcurrentSimulations = DefaultMevConfig.MaxConcurrentSimulations
	}
	if config.SimulationGasRate == 0 {
		config.SimulationGasRate = DefaultMevConfig.SimulationGasRate
	}
	if config.SimulationTimeout <= 0 {
		config.SimulationTimeout = DefaultMevConfig.SimulationTimeout
	}
	return &bundleSimulationLimiter{
		concurrency: config.MaxConcurrentSimulations,
		timeout:     config.SimulationTimeout,
		gas:         rate.NewLimiter(rate.Limit(config.SimulationGasRate), int(params.BundleGasLimit)),
		inflight:    make(map[simulationKey]struct{}),
	}
}

// start reserves declared gas, capped at the per-bundle execution limit. Work
// that fails or is canceled retains its debit: pool slots are not a CPU budget.
func (l *bundleSimulationLimiter) start(ctx context.Context, bundle *types.Bundle, gasless bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	key := simulationKey{hash: bundle.SimulationHash(), gasless: gasless}
	var gas uint64
	for _, tx := range bundle.Txs {
		declared := max(tx.Gas(), params.TxGas)
		if declared >= params.BundleGasLimit-gas {
			gas = params.BundleGasLimit
			break
		}
		gas += declared
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if _, ok := l.inflight[key]; ok {
		return nil, nil, ErrBundleSimulationBusy
	}
	if l.running >= l.concurrency || !l.gas.AllowN(time.Now(), int(gas)) {
		return nil, nil, ErrBundleSimulationBusy
	}
	l.running++
	l.inflight[key] = struct{}{}
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	return ctx, func() {
		cancel()
		l.mu.Lock()
		delete(l.inflight, key)
		l.running--
		l.mu.Unlock()
	}, nil
}

func (miner *Miner) startBundleSimulation(ctx context.Context, bundle *types.Bundle, gasless bool) (context.Context, func(), error) {
	miner.simulationOnce.Do(func() { miner.simulations = newBundleSimulationLimiter(miner.worker.config.Mev) })
	return miner.simulations.start(ctx, bundle, gasless)
}

// admissionTxPoolSnapshot freezes fee attribution for this request. Otherwise a
// transaction moving in/out of the ordinary pool during EVM execution changes
// its bundle price. The fee-cap maximum is a safe pre-execution price bound;
// a mixed bundle must still be simulated to find its gas-weighted price.
func (miner *Miner) admissionTxPoolSnapshot(bundle *types.Bundle) (map[common.Hash]bool, error) {
	public := make(map[common.Hash]bool, len(bundle.Txs))
	upper := new(big.Int)
	for _, tx := range bundle.Txs {
		known := miner.eth.TxPool().Has(tx.Hash())
		public[tx.Hash()] = known
		if !known && tx.GasFeeCap().Cmp(upper) > 0 {
			upper.Set(tx.GasFeeCap())
		}
	}
	if upper.Cmp(big.NewInt(miner.worker.config.Mev.MevBundleGasPriceFloor)) < 0 {
		return nil, errBundlePriceTooLow
	}
	return public, nil
}
