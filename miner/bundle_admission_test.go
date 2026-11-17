package miner

import (
	"context"
	"errors"
	"math/big"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/legacypool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type admissionTracer struct {
	evm     atomic.Pointer[vm.EVM]
	steps   atomic.Uint64
	starts  atomic.Int32
	cancel  context.CancelFunc
	entered chan struct{}
	release <-chan struct{}
}

func (t *admissionTracer) CaptureTxStart(uint64) {}
func (t *admissionTracer) CaptureTxEnd(uint64)   {}
func (t *admissionTracer) CaptureStart(evm *vm.EVM, _ common.Address, _ common.Address, _ bool, _ []byte, _ uint64, _ *big.Int) {
	t.evm.Store(evm)
	t.starts.Add(1)
	if t.entered != nil {
		t.entered <- struct{}{}
		<-t.release
	}
}
func (t *admissionTracer) CaptureEnd([]byte, uint64, error) {}
func (t *admissionTracer) CaptureEnter(vm.OpCode, common.Address, common.Address, []byte, uint64, *big.Int) {
}
func (t *admissionTracer) CaptureExit([]byte, uint64, error) {}
func (t *admissionTracer) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
	if t.steps.Add(1) == 64 && t.cancel != nil {
		t.cancel()
		// AfterFunc runs asynchronously. Let its callback run before resuming EVM
		// instructions, so this assertion does not depend on host scheduling speed.
		deadline := time.Now().Add(time.Second)
		for !t.evm.Load().Cancelled() && time.Now().Before(deadline) {
			runtime.Gosched()
		}
	}
}
func (t *admissionTracer) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {
}

func newAdmissionMiner(t *testing.T, tracer vm.EVMLogger) (*Miner, *testWorkerBackend) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	engine := ethash.NewFaker()
	genesis := &core.Genesis{Config: params.TestChainConfig, GasLimit: 30000000, Alloc: types.GenesisAlloc{testBankAddress: {Balance: testBankFunds}}}
	chain, err := core.NewBlockChain(db, &core.CacheConfig{TrieDirtyDisabled: true}, genesis, nil, engine, vm.Config{Tracer: tracer}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacypool.New(testTxPoolConfig, chain)
	pool, err := txpool.New(testTxPoolConfig.PriceLimit, chain, []txpool.SubPool{legacy})
	if err != nil {
		t.Fatal(err)
	}
	backend := &testWorkerBackend{db: db, chain: chain, txPool: pool, genesis: genesis}
	t.Cleanup(func() { pool.Close(); chain.Stop(); db.Close() })
	w := &worker{config: &Config{GasCeil: 30000000, Mev: MevConfig{MevEnabled: true, MevBundleGasPriceFloor: 9}}, chainConfig: params.TestChainConfig, engine: engine, eth: backend, chain: chain, bundleCache: NewBundleCache()}
	return &Miner{worker: w, eth: backend}, backend
}
func heavyAdmissionBundle(marker byte) *types.Bundle {
	// Loop until fewer than 256 gas remain, then STOP successfully.
	code := append(common.FromHex("0x5b5a6101001060005700"), marker)
	tx := types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Gas: params.BundleGasLimit, GasPrice: big.NewInt(params.InitialBaseFee), Data: code})
	return &types.Bundle{Txs: types.Transactions{tx}}
}

type admissionStateCounter struct {
	Backend
	reads atomic.Int32
}

func (b *admissionStateCounter) BlockChain() *core.BlockChain {
	b.reads.Add(1)
	return b.Backend.BlockChain()
}

func TestBundleAdmissionRejectsPublicTransactions(t *testing.T) {
	tracer := new(admissionTracer)
	m, b := newAdmissionMiner(t, tracer)
	counter := &admissionStateCounter{Backend: b}
	m.eth = counter
	bundle := heavyAdmissionBundle(0)
	if err := b.txPool.Add(bundle.Txs, true, true)[0]; err != nil {
		t.Fatal(err)
	}
	if _, err := m.SimulateBundle(context.Background(), bundle); !errors.Is(err, errBundlePriceTooLow) {
		t.Errorf("expected price rejection, got %v", err)
	}
	if got := tracer.starts.Load(); got != 0 {
		t.Errorf("all-txpool rejection executed EVM %d times", got)
	}
	if counter.reads.Load() != 0 {
		t.Fatal("all-txpool rejection prepared simulation state")
	}

}
func TestBundleAdmissionCancellation(t *testing.T) {
	for _, gasless := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "gasless"}[gasless], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tracer := &admissionTracer{cancel: cancel}
			m, b := newAdmissionMiner(t, tracer)
			var err error
			if gasless {
				_, err = m.SimulateGaslessBundle(ctx, heavyAdmissionBundle(0))
			} else {
				_, err = m.SimulateBundle(ctx, heavyAdmissionBundle(0))
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("want canceled, got %v", err)
			}
			if got := tracer.steps.Load(); got > 10000 {
				t.Errorf("canceled EVM continued for %d opcodes", got)
			}
			st, e := b.chain.StateAt(b.chain.CurrentBlock().Root)
			if e != nil {
				t.Fatal(e)
			}
			if st.GetNonce(testBankAddress) != 0 {
				t.Fatal("simulation modified base state")
			}
		})
	}
}
func TestBundleAdmissionSharedConcurrencyBudget(t *testing.T) {
	for _, gasless := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "gasless"}[gasless], func(t *testing.T) {
			entered, release := make(chan struct{}, 2), make(chan struct{})
			tracer := &admissionTracer{entered: entered, release: release}
			m, _ := newAdmissionMiner(t, tracer)
			first := make(chan error, 1)
			go func() { _, err := m.SimulateBundle(context.Background(), heavyAdmissionBundle(0)); first <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("simulation did not start")
			}
			second := make(chan error, 1)
			go func() {
				var err error
				if gasless {
					_, err = m.SimulateGaslessBundle(context.Background(), heavyAdmissionBundle(1))
				} else {
					_, err = m.SimulateBundle(context.Background(), heavyAdmissionBundle(1))
				}
				second <- err
			}()
			select {
			case <-entered:
				close(release)
				<-first
				<-second
				t.Fatal("overloaded RPC entered EVM")
			case err := <-second:
				close(release)
				<-first
				if err == nil || err.Error() != "bundle simulation busy" {
					t.Fatalf("want busy, got %v", err)
				}
			case <-time.After(3 * time.Second):
				close(release)
				<-first
				<-second
				t.Fatal("overloaded RPC waited instead of rejecting")
			}
		})
	}
}

func TestBundleAdmissionFailureConsumesGasBudget(t *testing.T) {
	m, _ := newAdmissionMiner(t, nil)
	m.worker.config.Mev.SimulationGasRate = 1
	invalid := types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Nonce: 1, Gas: params.BundleGasLimit, GasPrice: big.NewInt(params.InitialBaseFee)})
	_, err := m.SimulateBundle(context.Background(), &types.Bundle{Txs: types.Transactions{invalid}})
	if !errors.Is(err, core.ErrNonceTooHigh) {
		t.Fatalf("want nonce error, got %v", err)
	}
	if _, err = m.SimulateBundle(context.Background(), heavyAdmissionBundle(0)); !errors.Is(err, ErrBundleSimulationBusy) {
		t.Fatalf("failed request refunded its execution allowance: %v", err)
	}
	if _, err = m.SimulateGaslessBundle(context.Background(), heavyAdmissionBundle(1)); !errors.Is(err, ErrBundleSimulationBusy) {
		t.Fatalf("gasless did not share gas allowance: %v", err)
	}
	for _, entry := range m.worker.bundleCache.entries {
		if entry != nil {
			t.Fatal("failed public admission polluted payload cache")
		}
	}
}

func TestBundleSimulationLimiter(t *testing.T) {
	t.Run("zero config and cancellation", func(t *testing.T) {
		limiter := newBundleSimulationLimiter(MevConfig{})
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := limiter.start(canceled, heavyAdmissionBundle(0), false); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		ctx, finish, err := limiter.start(context.Background(), heavyAdmissionBundle(0), false)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("default timeout disabled")
		}
		finish()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("finish did not cancel simulation")
		}
	})
	t.Run("semantic duplicate does not spend budget", func(t *testing.T) {
		limiter := newBundleSimulationLimiter(MevConfig{MaxConcurrentSimulations: 2, SimulationGasRate: 1})
		bundle := admissionSmallBundle(0)
		_, finish, err := limiter.start(context.Background(), bundle, false)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		other := admissionSmallBundle(0)
		other.MaxBlockNumber = 99
		other.Price = big.NewInt(999)
		if _, _, err := limiter.start(context.Background(), other, false); !errors.Is(err, ErrBundleSimulationBusy) {
			t.Fatalf("equivalent execution admitted: %v", err)
		}
		_, secondFinish, err := limiter.start(context.Background(), admissionSmallBundle(1), false)
		if err != nil {
			t.Fatal("duplicate consumed concurrency:", err)
		}
		secondFinish()
	})
	t.Run("finish releases concurrency", func(t *testing.T) {
		limiter := newBundleSimulationLimiter(MevConfig{})
		_, finish, err := limiter.start(context.Background(), admissionSmallBundle(0), false)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := limiter.start(context.Background(), admissionSmallBundle(1), true); !errors.Is(err, ErrBundleSimulationBusy) {
			t.Fatal(err)
		}
		finish()
		_, finish, err = limiter.start(context.Background(), admissionSmallBundle(1), true)
		if err != nil {
			t.Fatal("concurrency leaked:", err)
		}
		finish()
	})
	t.Run("deadline", func(t *testing.T) {
		limiter := newBundleSimulationLimiter(MevConfig{SimulationTimeout: time.Millisecond})
		ctx, finish, err := limiter.start(context.Background(), admissionSmallBundle(0), false)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("no deadline")
		}
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal(ctx.Err())
		}
	})
}
func admissionSmallBundle(n uint64) *types.Bundle {
	return &types.Bundle{Txs: types.Transactions{types.NewTx(&types.LegacyTx{Nonce: n, Gas: 21000, GasPrice: big.NewInt(1)})}}
}
