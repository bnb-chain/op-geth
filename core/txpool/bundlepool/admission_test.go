package bundlepool

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/miner"
)

type admissionSimulator struct {
	calls atomic.Int32
	run   func(*types.Bundle) (*big.Int, error)
}

func (s *admissionSimulator) SimulateBundle(ctx context.Context, b *types.Bundle) (*big.Int, error) {
	s.calls.Add(1)
	if s.run != nil {
		return s.run(b)
	}
	return big.NewInt(1), nil
}
func admissionBundle(n uint64) *types.Bundle {
	return &types.Bundle{Txs: types.Transactions{types.NewTx(&types.LegacyTx{Nonce: n, Gas: 21000, GasPrice: new(big.Int).SetUint64(n + 1)})}}
}
func TestBundleAdmissionPrechecks(t *testing.T) {
	for _, name := range []string{"duplicate", "empty", "unknown revert", "oversized", "too many transactions", "exceeds pool capacity"} {
		t.Run(name, func(t *testing.T) {
			p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
			sim := new(admissionSimulator)
			p.SetBundleSimulator(sim)
			b := admissionBundle(0)
			switch name {
			case "duplicate":
				if err := p.AddBundle(context.Background(), b, nil); err != nil {
					t.Fatal(err)
				}
			case "empty":
				b.Txs = nil
			case "unknown revert":
				b.RevertingTxHashes = []common.Hash{{1}}
			case "oversized":
				b.Txs = types.Transactions{types.NewTx(&types.LegacyTx{Gas: 1000000, Data: make([]byte, 512*1024)})}
			case "too many transactions":
				b.Txs = nil
				for i := 0; i < 477; i++ {
					b.Txs = append(b.Txs, admissionBundle(uint64(i)).Txs[0])
				}
			case "exceeds pool capacity":
				b.Txs = types.Transactions{types.NewTx(&types.LegacyTx{Gas: 1000000, Data: make([]byte, bundleSlotSize+1)})}
			}
			before := sim.calls.Load()
			if err := p.AddBundle(context.Background(), b, nil); err == nil {
				t.Error("invalid admission accepted")
			}
			if got := sim.calls.Load() - before; got != 0 {
				t.Errorf("rejected request invoked simulator %d times", got)
			}
		})
	}
}
func TestBundleAdmissionReplacementAndUnlockedSimulation(t *testing.T) {
	p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
	sim := &admissionSimulator{run: func(b *types.Bundle) (*big.Int, error) {
		// Simulation failures can call PruneBundle; the admission mutex must be free.
		p.PruneBundle(common.Hash{})
		return big.NewInt(int64(b.Txs[0].Nonce() + 1)), nil
	}}
	p.SetBundleSimulator(sim)
	a, b := admissionBundle(0), admissionBundle(1)
	if err := p.AddBundle(context.Background(), a, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.AddBundle(context.Background(), b, nil); err != nil {
		t.Fatalf("higher-price replacement rejected: %v", err)
	}
	if p.GetBundle(a.Hash()) != nil || p.GetBundle(b.Hash()) == nil || p.slots != 1 {
		t.Fatal("incorrect replacement accounting")
	}
}
func TestBundleAdmissionConcurrentDuplicate(t *testing.T) {
	p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
	entered, release := make(chan struct{}, 2), make(chan struct{})
	sim := &admissionSimulator{run: func(*types.Bundle) (*big.Int, error) {
		entered <- struct{}{}
		<-release
		return nil, errors.New("rejected")
	}}
	p.SetBundleSimulator(sim)
	result := make(chan error, 2)
	go func() { result <- p.AddBundle(context.Background(), admissionBundle(0), nil) }()
	<-entered
	go func() { result <- p.AddBundle(context.Background(), admissionBundle(0), nil) }()
	select {
	case <-entered:
		close(release)
		<-result
		<-result
		t.Fatal("duplicate entered simulator concurrently")
	case err := <-result:
		if !errors.Is(err, ErrBundleAlreadyExist) {
			t.Errorf("unexpected duplicate error: %v", err)
		}
		close(release)
		<-result
	}
	if p.slots != 0 {
		t.Fatal("failed simulations consumed slots")
	}
}

func TestBundleAdmissionRejectsProvablyUnderpricedReplacement(t *testing.T) {
	p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
	sim := new(admissionSimulator)
	p.SetBundleSimulator(sim)
	if err := p.AddBundle(context.Background(), admissionBundle(0), nil); err != nil {
		t.Fatal(err)
	}
	before := sim.calls.Load()
	underpriced := &types.Bundle{Txs: types.Transactions{types.NewTx(&types.LegacyTx{Nonce: 1, Gas: 21000, GasPrice: big.NewInt(1)})}}
	if err := p.AddBundle(context.Background(), underpriced, nil); !errors.Is(err, ErrBundleGasPriceLow) {
		t.Fatalf("want price rejection, got %v", err)
	}
	if sim.calls.Load() != before {
		t.Fatal("provably underpriced replacement entered simulator")
	}
}

func TestBundleAdmissionCancellationDoesNotInsert(t *testing.T) {
	p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	sim := &admissionSimulator{run: func(*types.Bundle) (*big.Int, error) { cancel(); return big.NewInt(1), nil }}
	p.SetBundleSimulator(sim)
	bundle := admissionBundle(0)
	if err := p.AddBundle(ctx, bundle, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	if p.slots != 0 || p.GetBundle(bundle.Hash()) != nil {
		t.Fatal("canceled admission inserted bundle")
	}
	if err := p.AddBundle(context.Background(), bundle, nil); err != nil {
		t.Fatalf("in-flight reservation leaked: %v", err)
	}
}

func TestBundleAdmissionRechecksCapacityAfterSimulation(t *testing.T) {
	p := setupBundlePool(Config{GlobalSlots: 1}, miner.MevConfig{})
	replacement := admissionBundle(1)
	sim := &admissionSimulator{run: func(b *types.Bundle) (*big.Int, error) {
		if b.Txs[0].Nonce() == 0 {
			if err := p.AddBundle(context.Background(), replacement, nil); err != nil {
				return nil, err
			}
			return big.NewInt(1), nil
		}
		return big.NewInt(2), nil
	}}
	p.SetBundleSimulator(sim)
	if err := p.AddBundle(context.Background(), admissionBundle(0), nil); !errors.Is(err, ErrBundleGasPriceLow) {
		t.Fatalf("want replacement rejection, got %v", err)
	}
	if p.slots != 1 || len(p.bundles) != 1 || p.GetBundle(replacement.Hash()) == nil {
		t.Fatal("admission used stale capacity")
	}
}
