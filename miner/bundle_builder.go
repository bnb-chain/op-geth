package miner

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

const (
	bundleBuilderWorkers    = 2
	bundleBuilderCandidates = 64
	bundleBuilderGas        = 2 * params.BundleGasLimit
)

// selectBuilderBundles reserves bounded work for both resimulation and merging.
// Hashes break price ties independently of pool map iteration; equivalent variants only
// spend one reservation. Accumulation saturates before addition can overflow.
func selectBuilderBundles(ctx context.Context, bundles []*types.Bundle) ([]*types.Bundle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ordered := slices.Clone(bundles)
	slices.SortFunc(ordered, func(a, b *types.Bundle) int {
		if cmp := b.Price.Cmp(a.Price); cmp != 0 {
			return cmp
		}
		ah, bh := a.Hash(), b.Hash()
		return bytes.Compare(ah[:], bh[:])
	})
	selected := make([]*types.Bundle, 0, min(len(ordered), bundleBuilderCandidates))
	seen := make(map[common.Hash]struct{})
	remaining := uint64(bundleBuilderGas)
	for _, bundle := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := bundle.SimulationHash()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var gas uint64
		for _, tx := range bundle.Txs {
			if tx.Gas() >= params.BundleGasLimit-gas {
				gas = params.BundleGasLimit
				break
			}
			gas += tx.Gas()
		}
		gas = max(gas, params.TxGas)
		if gas > remaining {
			continue
		}
		remaining -= gas
		selected = append(selected, bundle)
		if len(selected) == bundleBuilderCandidates {
			break
		}
	}
	return selected, nil
}

// bundleBuildContext is local to one generation; the parent deadline is fixed
// for the entire payload, so recommits cannot extend its execution allowance.
func (w *worker) bundleBuildContext(parent context.Context, started time.Time, interval time.Duration) (context.Context, context.CancelFunc) {
	if w.newpayloadTimeout > 0 && w.newpayloadTimeout < interval {
		interval = w.newpayloadTimeout
	}
	return context.WithDeadline(parent, started.Add(interval/2))
}
