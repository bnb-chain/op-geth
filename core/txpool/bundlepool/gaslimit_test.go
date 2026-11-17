package bundlepool

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/miner"
)

func gasLimitBundle(txType uint8, gas ...uint64) *types.Bundle {
	bundle := new(types.Bundle)
	for i, limit := range gas {
		var tx types.TxData
		switch txType {
		case types.LegacyTxType:
			tx = &types.LegacyTx{Nonce: uint64(i), Gas: limit, GasPrice: big.NewInt(1)}
		case types.AccessListTxType:
			tx = &types.AccessListTx{Nonce: uint64(i), Gas: limit, GasPrice: big.NewInt(1)}
		case types.DynamicFeeTxType:
			tx = &types.DynamicFeeTx{Nonce: uint64(i), Gas: limit, GasFeeCap: big.NewInt(1), GasTipCap: big.NewInt(1)}
		default:
			panic("unsupported test transaction type")
		}
		bundle.Txs = append(bundle.Txs, types.NewTx(tx))
	}
	return bundle
}

func TestBundleTxGasLimitAdmission(t *testing.T) {
	for _, txType := range []struct {
		name string
		id   uint8
	}{
		{"legacy", types.LegacyTxType},
		{"access_list", types.AccessListTxType},
		{"dynamic_fee", types.DynamicFeeTxType},
	} {
		for _, tc := range []struct {
			name   string
			cap    uint64
			gas    []uint64
			reject bool
		}{
			{"disabled", 0, []uint64{50_001}, false},
			{"below_cap", 50_000, []uint64{49_999}, false},
			{"at_cap", 50_000, []uint64{50_000}, false},
			{"above_cap", 50_000, []uint64{50_001}, true},
			{"non_first_above_cap", 50_000, []uint64{21_000, 50_001}, true},
			{"per_transaction_not_total", 50_000, []uint64{30_000, 30_000}, false},
		} {
			t.Run(txType.name+"/"+tc.name, func(t *testing.T) {
				p := setupBundlePool(DefaultConfig, miner.MevConfig{})
				sim := new(admissionSimulator)
				p.SetBundleSimulator(sim)
				p.SetMaxTxGas(50_000)
				p.SetMaxTxGas(tc.cap)
				bundle := gasLimitBundle(txType.id, tc.gas...)
				if !p.FilterBundle(bundle) {
					t.Fatal("supported transaction type was not routed to BundlePool")
				}
				err := p.AddBundle(context.Background(), bundle, nil)
				if tc.reject {
					if !errors.Is(err, core.ErrTxGasLimitTooHigh) {
						t.Fatalf("got %v, want ErrTxGasLimitTooHigh", err)
					}
					if sim.calls.Load() != 0 || p.GetBundle(bundle.Hash()) != nil || p.slots != 0 || len(p.inflight) != 0 {
						t.Fatal("over-limit bundle invoked simulation or consumed pool capacity")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if sim.calls.Load() != 1 || p.GetBundle(bundle.Hash()) == nil {
						t.Fatal("valid bundle was not simulated and admitted")
					}
				}
			})
		}
	}
}

type gasLimitTestChain struct {
	*testBlockChain
	headFeed event.Feed
}

func (c *gasLimitTestChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return c.headFeed.Subscribe(ch)
}

func TestBundleTxGasLimitRouting(t *testing.T) {
	p := setupBundlePool(DefaultConfig, miner.MevConfig{})
	sim := new(admissionSimulator)
	p.SetBundleSimulator(sim)
	chain := &gasLimitTestChain{testBlockChain: p.chain.(*testBlockChain)}
	pool, err := txpool.New(1, chain, []txpool.SubPool{p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	pool.SetMaxTxGas(50_000)
	if err := pool.AddBundle(context.Background(), gasLimitBundle(types.LegacyTxType, 50_001), nil); !errors.Is(err, core.ErrTxGasLimitTooHigh) {
		t.Fatalf("outer TxPool returned %v, want ErrTxGasLimitTooHigh", err)
	}
	if sim.calls.Load() != 0 {
		t.Fatal("over-limit bundle reached simulation through the outer TxPool")
	}
}

func TestBundleTxGasLimitUpdateDuringSimulation(t *testing.T) {
	p := setupBundlePool(DefaultConfig, miner.MevConfig{})
	p.SetMaxTxGas(50_000)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p.SetBundleSimulator(&admissionSimulator{run: func(*types.Bundle) (*big.Int, error) {
		close(entered)
		<-release
		return big.NewInt(1), nil
	}})
	bundle := gasLimitBundle(types.LegacyTxType, 40_000)
	result := make(chan error, 1)
	go func() { result <- p.AddBundle(context.Background(), bundle, nil) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("simulation did not start")
	}
	p.SetMaxTxGas(30_000)
	close(release)
	select {
	case err := <-result:
		if !errors.Is(err, core.ErrTxGasLimitTooHigh) {
			t.Fatalf("got %v, want ErrTxGasLimitTooHigh after cap reduction", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not finish")
	}
	if p.slots != 0 || p.GetBundle(bundle.Hash()) != nil || len(p.inflight) != 0 {
		t.Fatal("bundle invalidated during simulation consumed pool capacity")
	}
	p.SetMaxTxGas(0)
	p.SetBundleSimulator(new(admissionSimulator))
	if err := p.AddBundle(context.Background(), bundle, nil); err != nil {
		t.Fatalf("rejected admission leaked its in-flight reservation: %v", err)
	}
}

func TestBundleTxGasLimitPending(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial uint64
		updated uint64
	}{
		{"enable", 0, 30_000},
		{"lower", 50_000, 30_000},
		{"disable", 50_000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := setupBundlePool(DefaultConfig, miner.MevConfig{})
			p.SetMaxTxGas(tc.initial)
			p.SetBundleSimulator(new(admissionSimulator))
			valid := gasLimitBundle(types.LegacyTxType, 30_000)
			invalid := gasLimitBundle(types.LegacyTxType, 21_000, 40_000)
			future := gasLimitBundle(types.LegacyTxType, 40_000)
			future.MinTimestamp = 100
			for _, bundle := range []*types.Bundle{valid, invalid, future} {
				if err := p.AddBundle(context.Background(), bundle, nil); err != nil {
					t.Fatal(err)
				}
			}
			p.SetMaxTxGas(tc.updated)
			pending := p.PendingBundles(1, 50)
			if tc.updated == 0 {
				if len(pending) != 2 || len(p.AllBundles()) != 3 || p.slots != numSlots(valid)+numSlots(invalid)+numSlots(future) {
					t.Fatal("disabled cap filtered a bundle or corrupted slot accounting")
				}
				return
			}
			if len(pending) != 1 || pending[0].Hash() != valid.Hash() {
				t.Fatalf("pending selection returned %d bundles, want only the at-cap bundle", len(pending))
			}
			if p.GetBundle(invalid.Hash()) != nil || p.GetBundle(future.Hash()) != nil || p.slots != numSlots(valid) {
				t.Fatal("over-limit pending bundles were not pruned with correct slot accounting")
			}
		})
	}
}
