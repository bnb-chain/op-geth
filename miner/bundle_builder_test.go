package miner

import (
	"context"
	"errors"
	"math"
	"math/big"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/legacypool"
	"github.com/ethereum/go-ethereum/core/types"
)

type builderTestPool struct {
	txpool.SubPool
	bundles []*types.Bundle
	pruned  atomic.Int32
}

func (p *builderTestPool) FilterBundle(*types.Bundle) bool { return true }
func (p *builderTestPool) AddBundle(context.Context, *types.Bundle, *types.SendBundleArgs) error {
	return nil
}
func (p *builderTestPool) PendingBundles(uint64, uint64) []*types.Bundle { return p.bundles }
func (p *builderTestPool) AllBundles() []*types.Bundle                   { return p.bundles }
func (p *builderTestPool) PruneBundle(common.Hash)                       { p.pruned.Add(1) }
func installBuilderTestPool(t *testing.T, b *testWorkerBackend, bundles []*types.Bundle) *builderTestPool {
	t.Helper()
	p := &builderTestPool{SubPool: legacypool.New(testTxPoolConfig, b.chain), bundles: bundles}
	pool, err := txpool.New(testTxPoolConfig.PriceLimit, b.chain, []txpool.SubPool{p})
	if err != nil {
		t.Fatal(err)
	}
	b.txPool = pool
	t.Cleanup(func() { pool.Close() })
	return p
}

func TestBundleBuilderCancellationDoesNotPrune(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracer := &admissionTracer{cancel: cancel}
	m, b := newAdmissionMiner(t, tracer)
	bundle := heavyAdmissionBundle(0)
	bundle.Price = big.NewInt(1)
	p := installBuilderTestPool(t, b, []*types.Bundle{bundle})
	env, err := m.prepareSimulationEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	_, err = m.worker.simulateBundleWithContext(ctx, env, bundle, env.state.Copy(), prepareGasPool(), 0, true, true, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation, got %v", err)
	}
	if p.pruned.Load() != 0 {
		t.Fatal("canceled valid bundle was pruned")
	}
}

func TestBundleBuilderTentativeMergeDoesNotPrune(t *testing.T) {
	m, b := newAdmissionMiner(t, nil)
	var bundles []*types.Bundle
	for _, marker := range []byte{1, 2} {
		// Both alternatives are valid at the parent, but use the same nonce.
		tx := types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{
			Gas: 100000, GasPrice: big.NewInt(params.InitialBaseFee), Data: []byte{0, marker},
		})
		bundles = append(bundles, &types.Bundle{Txs: types.Transactions{tx}, Price: big.NewInt(1)})
	}
	p := installBuilderTestPool(t, b, bundles)
	env, err := m.prepareSimulationEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	var simulated []*types.SimulatedBundle
	for _, bundle := range bundles {
		result, err := m.worker.simulateBundleWithContext(context.Background(), env, bundle, env.state.Copy(), prepareGasPool(), 0, false, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		simulated = append(simulated, result)
	}
	txs, _, err := m.worker.mergeBundles(context.Background(), env, simulated)
	if err != nil || len(txs) != 1 {
		t.Fatalf("want one tentative alternative, got %d transactions: %v", len(txs), err)
	}
	// This tentative result may be discarded on timeout. A merge-only conflict
	// must not remove an alternative from the shared pool before publication.
	if p.pruned.Load() != 0 {
		t.Fatal("tentative merge pruned an alternative valid at the parent")
	}
}

func builderCandidate(n, gas uint64, price int64) *types.Bundle {
	b := admissionSmallBundle(n)
	b.Txs = types.Transactions{types.NewTx(&types.LegacyTx{Nonce: n, Gas: gas, GasPrice: big.NewInt(1)})}
	b.Price = big.NewInt(price)
	return b
}
func TestBundleBuilderCandidateLimits(t *testing.T) {
	t.Run("gas skip and stable price", func(t *testing.T) {
		first := builderCandidate(0, params.BundleGasLimit, 3)
		variant := builderCandidate(0, params.BundleGasLimit, 4)
		variant.MaxBlockNumber = 999
		second := builderCandidate(1, params.BundleGasLimit-params.TxGas, 3)
		tooLarge := builderCandidate(2, params.TxGas+1, 2)
		last := builderCandidate(3, 0, 1)
		got, err := selectBuilderBundles(context.Background(), []*types.Bundle{first, second, tooLarge, variant, last})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0] != variant || got[1] != second || got[2] != last {
			t.Fatalf("wrong selected candidates: %v", got)
		}
	})
	t.Run("count", func(t *testing.T) {
		var bundles []*types.Bundle
		for i := uint64(0); i < 80; i++ {
			bundles = append(bundles, builderCandidate(i, params.TxGas, 1))
		}
		got, err := selectBuilderBundles(context.Background(), bundles)
		if err != nil || len(got) != 64 {
			t.Fatalf("got %d: %v", len(got), err)
		}
		slices.Reverse(bundles)
		again, err := selectBuilderBundles(context.Background(), bundles)
		if err != nil {
			t.Fatal(err)
		}
		for i, b := range got {
			if b != again[i] {
				t.Fatal("tie order depends on input map order")
			}
		}
	})
	t.Run("semantic variants", func(t *testing.T) {
		a := builderCandidate(0, params.TxGas, 1)
		b := builderCandidate(0, params.TxGas, 1)
		b.RevertingTxHashes = []common.Hash{b.Txs[0].Hash()}
		got, err := selectBuilderBundles(context.Background(), []*types.Bundle{a, b})
		if err != nil || len(got) != 2 {
			t.Fatalf("reverting semantics deduplicated: %v", err)
		}
	})
	t.Run("overflow", func(t *testing.T) {
		a := builderCandidate(0, math.MaxUint64, 2)
		a.Txs = append(a.Txs, types.NewTx(&types.LegacyTx{Gas: math.MaxUint64}))
		got, err := selectBuilderBundles(context.Background(), []*types.Bundle{a, builderCandidate(1, math.MaxUint64, 1), builderCandidate(2, params.TxGas, 1)})
		if err != nil || len(got) != 2 {
			t.Fatalf("overflow bypassed gas allowance: %d %v", len(got), err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := selectBuilderBundles(ctx, []*types.Bundle{builderCandidate(0, 1, 1)}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestBundleBuilderDeadline(t *testing.T) {
	w := &worker{newpayloadTimeout: 650 * time.Millisecond}
	interval := 250 * time.Millisecond
	start := time.Now()
	ctx, cancel := w.bundleBuildContext(context.Background(), start, interval)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if !deadline.Equal(start.Add(125 * time.Millisecond)) {
		t.Fatalf("250ms block budget: %v", deadline.Sub(start))
	}
	parent, stop := context.WithDeadline(context.Background(), start.Add(20*time.Millisecond))
	defer stop()
	ctx, cancel = w.bundleBuildContext(parent, start, interval)
	defer cancel()
	deadline, _ = ctx.Deadline()
	if !deadline.Equal(start.Add(20 * time.Millisecond)) {
		t.Fatal("parent deadline extended")
	}
	w.newpayloadTimeout = 100 * time.Millisecond
	ctx, cancel = w.bundleBuildContext(context.Background(), start, interval)
	defer cancel()
	deadline, _ = ctx.Deadline()
	if !deadline.Equal(start.Add(50 * time.Millisecond)) {
		t.Fatal("configured timeout ignored")
	}
	for _, timeout := range []time.Duration{0, -time.Millisecond} {
		w.newpayloadTimeout = timeout
		ctx, cancel = w.bundleBuildContext(context.Background(), start, interval)
		defer cancel()
		deadline, _ = ctx.Deadline()
		if !deadline.Equal(start.Add(125 * time.Millisecond)) {
			t.Fatal("nonpositive configuration disabled interval budget")
		}
	}
}

// The barrier observes real EVM entry; cancellation then releases both workers.
// Return implies both executing EVMs and the queued work have been joined.
func TestBundleBuilderWorkersCancelAndJoin(t *testing.T) {
	entered, release := make(chan struct{}, 8), make(chan struct{})
	tracer := &admissionTracer{entered: entered, release: release}
	m, b := newAdmissionMiner(t, tracer)
	var bundles []*types.Bundle
	for i := byte(0); i < 8; i++ {
		x := heavyAdmissionBundle(i)
		x.Txs = types.Transactions{types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Gas: 1000000, GasPrice: big.NewInt(params.InitialBaseFee), Data: x.Txs[0].Data()})}
		x.Price = big.NewInt(1)
		bundles = append(bundles, x)
	}
	p := installBuilderTestPool(t, b, bundles)
	env, err := m.prepareSimulationEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.worker.simulateBundles(ctx, env, bundles); done <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("two workers did not enter EVM")
		}
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not exit")
	}
	if got := tracer.starts.Load(); got != 2 {
		t.Fatalf("started %d EVMs, want 2", got)
	}
	if p.pruned.Load() != 0 {
		t.Fatal("canceled worker pruned bundles")
	}
	cache := m.worker.bundleCache.GetBundleCache(env.header.Hash())
	for _, b := range bundles {
		if _, ok := cache.GetSimulatedBundle(b.Hash()); ok {
			t.Fatal("interrupted/queued work cached as invalid")
		}
	}
}
func TestBundleBuilderActiveEVMCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracer := &admissionTracer{cancel: cancel}
	m, _ := newAdmissionMiner(t, tracer)
	env, err := m.prepareSimulationEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	b := heavyAdmissionBundle(0)
	b.Price = big.NewInt(1)
	_, err = m.worker.simulateBundles(ctx, env, []*types.Bundle{b})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if tracer.steps.Load() > 10000 {
		t.Fatal("active EVM was not canceled")
	}
}

type builderCreateTracer struct{ admissionTracer }

func (t *builderCreateTracer) CaptureStart(evm *vm.EVM, from, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	if create {
		t.admissionTracer.CaptureStart(evm, from, to, create, input, gas, value)
	}
}

func TestBundleBuilderNonpositiveTimeoutIncludesBundle(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			entered, release := make(chan struct{}, 4), make(chan struct{})
			tracer := &builderCreateTracer{admissionTracer: admissionTracer{entered: entered, release: release}}
			m, b := newAdmissionMiner(t, tracer)
			w := m.worker
			// Exercise zero directly: the normal constructor replaces it with the
			// default timeout, while negative configured values remain accepted.
			w.newpayloadTimeout = timeout
			tx := types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Gas: 100000, GasPrice: big.NewInt(params.InitialBaseFee), Data: []byte{0}})
			bundle := &types.Bundle{Txs: types.Transactions{tx}, Price: big.NewInt(1)}
			p := installBuilderTestPool(t, b, []*types.Bundle{bundle})
			interrupt := new(atomic.Int32)
			done := make(chan *newPayloadResult, 1)
			go func() {
				// A generous requested interval isolates this timer edge from the
				// separately tested 250ms scheduling budget and host throughput.
				done <- w.generateWork(&generateParams{interrupt: interrupt, timestamp: 60, forceTime: true, random: common.Hash{3: 2}})
			}()
			var result *newPayloadResult
			select {
			case <-entered:
				// Hold real EVM execution briefly so an erroneously installed
				// nonpositive timer can run before the cheap STOP completes.
				<-time.After(20 * time.Millisecond)
			case result = <-done:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("generation did not execute or return")
			}
			close(release)
			if result == nil {
				select {
				case result = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("generation did not join bundle execution")
				}
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			if interrupt.Load() != commitInterruptNone {
				t.Errorf("nonpositive configured timeout interrupted generation: %d", interrupt.Load())
			}
			if tracer.starts.Load() == 0 {
				t.Error("bundle did not enter the EVM")
			}
			if txs := result.block.Transactions(); len(txs) != 1 || txs[0].Hash() != tx.Hash() {
				t.Fatalf("nonpositive configured timeout lost valid bundle: got %d transactions", len(txs))
			}
			if p.pruned.Load() != 0 {
				t.Fatal("valid bundle was pruned")
			}
			if _, err := b.chain.InsertChain(types.Blocks{result.block}); err != nil {
				t.Fatalf("invalid bundle block: %v", err)
			}
		})
	}
}

type builderEndTracer struct {
	admissionTracer
	cancel context.CancelFunc
	once   sync.Once
}

func (t *builderEndTracer) CaptureTxEnd(uint64) { t.once.Do(t.cancel) }
func TestBundleBuilderMergeAndCommitCancellation(t *testing.T) {
	for _, phase := range []string{"merge", "commit"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tracer := &builderEndTracer{cancel: cancel}
			m, b := newAdmissionMiner(t, tracer)
			p := installBuilderTestPool(t, b, nil)
			env, err := m.prepareSimulationEnv(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer env.discard()
			tx := types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Gas: 100000, GasPrice: big.NewInt(params.InitialBaseFee), Data: []byte{0}})
			bundle := &types.Bundle{Txs: types.Transactions{tx}, Price: big.NewInt(1)}
			if phase == "merge" {
				_, _, err = m.worker.mergeBundles(ctx, env, []*types.SimulatedBundle{{OriginalBundle: bundle, BundleGasPrice: big.NewInt(1)}})
			} else {
				env.UnRevertible = mapset.NewSet[common.Hash](tx.Hash())
				err = m.worker.commitBundles(ctx, env, bundle.Txs, nil)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("want cancellation after transaction finalise, got %v", err)
			}
			if len(env.txs) != 0 {
				t.Fatal("canceled transaction appended")
			}
			if p.pruned.Load() != 0 {
				t.Fatal("canceled merge pruned bundle")
			}
		})
	}
}
func TestBundleBuilderPayloadFallback(t *testing.T) {
	for _, resolve := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "resolve"}[resolve], func(t *testing.T) {
			entered, release := make(chan struct{}, 4), make(chan struct{})
			tracer := &builderCreateTracer{admissionTracer: admissionTracer{entered: entered, release: release}}
			db := rawdb.NewMemoryDatabase()
			cfg := *testConfig
			cfg.Mev.MevEnabled = true
			cfg.NewPayloadTimeout = 650 * time.Millisecond
			w, b := newTestWorker(t, params.TestChainConfig, ethash.NewFaker(), db, 0, &cfg, &vm.Config{Tracer: tracer})
			defer w.close()
			originalPool := b.txPool
			t.Cleanup(func() { originalPool.Close(); b.chain.Stop(); db.Close() })
			// A forced transfer is retained when the private contract creation is canceled.
			bundle := heavyAdmissionBundle(0)
			bundle.Txs = types.Transactions{types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Nonce: 1, Gas: params.BundleGasLimit, GasPrice: big.NewInt(params.InitialBaseFee), Data: bundle.Txs[0].Data()})}
			forced := genTxs(0, 1)
			bundle.Price = big.NewInt(1)
			p := installBuilderTestPool(t, b, []*types.Bundle{bundle})
			payload, err := w.buildPayload(&BuildPayloadArgs{Parent: b.chain.CurrentBlock().Hash(), Timestamp: b.chain.CurrentBlock().Time + 1, Random: common.Hash{3: 1}, Transactions: forced})
			if err != nil {
				close(release)
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("bundle did not execute")
			}
			result := make(chan *engine.ExecutionPayloadEnvelope, 1)
			if resolve {
				go func() { result <- payload.Resolve() }()
			} else {
				<-payload.ctx.Done()
			}
			close(release)
			if !resolve {
				go func() { result <- payload.Resolve() }()
			}
			var out *engine.ExecutionPayloadEnvelope
			select {
			case out = <-result:
			case <-time.After(3 * time.Second):
				t.Fatal("initial Resolve blocked on bundle work")
			}
			if out == nil || len(out.ExecutionPayload.Transactions) == 0 {
				t.Fatal("missing ordinary fallback payload")
			}
			for _, raw := range out.ExecutionPayload.Transactions {
				tx := new(types.Transaction)
				if err := tx.UnmarshalBinary(raw); err != nil {
					t.Fatal(err)
				}
				if tx.Hash() == bundle.Txs[0].Hash() {
					t.Fatal("partial bundle published")
				}
			}
			if p.pruned.Load() != 0 {
				t.Fatal("timeout pruned bundle")
			}
			block := payload.GetBlock()
			if _, err := b.chain.InsertChain(types.Blocks{block}); err != nil {
				t.Fatalf("invalid fallback block: %v", err)
			}
		})
	}
}

func TestBundleBuilderExpiredPayloadFallback(t *testing.T) {
	m, b := newAdmissionMiner(t, nil)
	m.worker.newpayloadTimeout = 650 * time.Millisecond
	payload := newPayload(nil, engine.PayloadID{}, time.Now().Add(-time.Second))
	defer payload.stopBuilding()
	forced := genTxs(0, 1)
	result := m.worker.generateWork(&generateParams{ctx: payload.ctx, interrupt: payload.interrupt, timestamp: 1, forceTime: true, random: common.Hash{3: 1}, txs: forced})
	if result.err != nil {
		t.Fatal(result.err)
	}
	payload.update(result, 0)
	out := payload.Resolve()
	if out == nil || len(out.ExecutionPayload.Transactions) != 1 {
		t.Fatal("expired initial attempt lost forced fallback")
	}
	if _, err := b.chain.InsertChain(types.Blocks{result.block}); err != nil {
		t.Fatal(err)
	}
}

// Hold the initial request in the worker queue until the absolute payload
// deadline has expired. Recommits must not replace that expired parent context,
// and Resolve must still receive the forced-transaction fallback.
func TestBundleBuilderInitialRequestPastDeadline(t *testing.T) {
	m, b := newAdmissionMiner(t, nil)
	w := m.worker
	w.newpayloadTimeout = 650 * time.Millisecond
	w.recommit = time.Second
	w.getWorkCh = make(chan *getWorkReq)
	w.exitCh = make(chan struct{})
	payload, err := w.buildPayload(&BuildPayloadArgs{Parent: b.chain.CurrentBlock().Hash(), Random: common.Hash{1: 250, 3: 1}, Transactions: genTxs(0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer payload.stopBuilding()
	var req *getWorkReq
	select {
	case req = <-w.getWorkCh:
	case <-time.After(3 * time.Second):
		t.Fatal("initial build never attempted")
	}
	<-payload.ctx.Done()
	deadline, _ := payload.ctx.Deadline()
	result := make(chan *engine.ExecutionPayloadEnvelope, 1)
	go func() { result <- payload.Resolve() }()
	if req.params.ctx != payload.ctx {
		t.Fatal("initial request lost payload context")
	}
	gotDeadline, _ := req.params.ctx.Deadline()
	if !gotDeadline.Equal(deadline) {
		t.Fatal("queued generation extended payload deadline")
	}
	built := w.generateWork(req.params)
	req.result <- built
	select {
	case out := <-result:
		if out == nil || len(out.ExecutionPayload.Transactions) != 1 {
			t.Fatal("Resolve lost initial fallback")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Resolve blocked after expired initial build")
	}
	if built.err != nil {
		t.Fatal(built.err)
	}
	if _, err := b.chain.InsertChain(types.Blocks{built.block}); err != nil {
		t.Fatal(err)
	}
}

type builderNthEndTracer struct {
	admissionTracer
	completed atomic.Int32
	cancelAt  int32
	cancel    context.CancelFunc
}

func (t *builderNthEndTracer) CaptureTxEnd(uint64) {
	if t.completed.Add(1) == t.cancelAt {
		t.cancel()
	}
}
func TestBundleBuilderCommitDiscardsPartialBundle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// One forced transaction, two resimulations, two merge executions, then
	// cancel the second commit. The first committed bundle transaction must go.
	tracer := &builderNthEndTracer{cancelAt: 7, cancel: cancel}
	m, b := newAdmissionMiner(t, tracer)
	w := m.worker
	w.newpayloadTimeout = time.Minute
	bundle := &types.Bundle{Price: big.NewInt(1)}
	for nonce := uint64(1); nonce <= 2; nonce++ {
		bundle.Txs = append(bundle.Txs, types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Nonce: nonce, Gas: 100000, GasPrice: big.NewInt(params.InitialBaseFee), Data: []byte{0}}))
	}
	p := installBuilderTestPool(t, b, []*types.Bundle{bundle})
	result := w.generateWork(&generateParams{ctx: ctx, timestamp: 1, forceTime: true, random: common.Hash{3: 255}, txs: genTxs(0, 1)})
	if result.err != nil {
		t.Fatal(result.err)
	}
	if tracer.completed.Load() != 7 || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("did not reach cancellation during final bundle commit")
	}
	if len(result.block.Transactions()) != 1 || len(result.env.receipts) != 1 {
		t.Fatal("partially committed bundle escaped fallback")
	}
	if p.pruned.Load() != 0 {
		t.Fatal("commit cancellation pruned bundle")
	}
	if _, err := b.chain.InsertChain(types.Blocks{result.block}); err != nil {
		t.Fatal(err)
	}
}

func TestBundleBuilderExpiredParentDiscardsUpdate(t *testing.T) {
	m, _ := newAdmissionMiner(t, nil)
	m.worker.newpayloadTimeout = 650 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	interrupt := new(atomic.Int32)
	result := m.worker.generateWork(&generateParams{ctx: ctx, interrupt: interrupt, isUpdate: true, timestamp: 1, forceTime: true, random: common.Hash{3: 1}, txs: genTxs(0, 1)})
	if interrupt.Load() != commitInterruptNone {
		t.Fatal("test requires an unset atomic interrupt")
	}
	if !errors.Is(result.err, errInterruptedUpdate) {
		t.Fatalf("expired parent update was finalized: %v", result.err)
	}
	if result.block != nil {
		t.Fatal("expired update returned a block")
	}
}

func TestBundleBuilderLiveParentLocalTimeoutFallback(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	tracer := &builderCreateTracer{admissionTracer: admissionTracer{entered: entered, release: release}}
	m, b := newAdmissionMiner(t, tracer)
	w := m.worker
	w.newpayloadTimeout = time.Minute
	bundle := heavyAdmissionBundle(0)
	bundle.Txs = types.Transactions{types.MustSignNewTx(testBankKey, types.LatestSigner(params.TestChainConfig), &types.LegacyTx{Nonce: 1, Gas: params.BundleGasLimit, GasPrice: big.NewInt(params.InitialBaseFee), Data: bundle.Txs[0].Data()})}
	bundle.Price = big.NewInt(1)
	installBuilderTestPool(t, b, []*types.Bundle{bundle})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupt := new(atomic.Int32)
	done := make(chan *newPayloadResult, 1)
	go func() {
		done <- w.generateWork(&generateParams{ctx: parent, interrupt: interrupt, isUpdate: true, timestamp: 1, forceTime: true, random: common.Hash{3: 1}, txs: genTxs(0, 1)})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("bundle EVM did not start")
	}
	// The tracer holds a real EVM until its local deadline cancels it. This is
	// a lifecycle assertion, independent of opcode throughput or host speed.
	deadline := time.Now().Add(3 * time.Second)
	for !tracer.evm.Load().Cancelled() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	canceled := tracer.evm.Load().Cancelled()
	close(release)
	var result *newPayloadResult
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("local timeout did not join generation")
	}
	if !canceled {
		t.Fatal("local deadline did not cancel EVM")
	}
	if parent.Err() != nil || interrupt.Load() != commitInterruptNone {
		t.Fatal("test requires live parent and unset atomic interrupt")
	}
	if result.err != nil {
		t.Fatalf("local bundle timeout discarded ordinary fallback: %v", result.err)
	}
	if len(result.block.Transactions()) != 1 {
		t.Fatal("local timeout lost forced fallback")
	}
	if _, err := b.chain.InsertChain(types.Blocks{result.block}); err != nil {
		t.Fatal(err)
	}
}

// op-node encodes the millisecond target in Random[:2] and interval/250+1
// in Random[3]. The requested timestamp span, not the raw interval byte,
// controls payload and bundle scheduling.
func requestedTimeRandom(millis uint64) common.Hash {
	return common.Hash{0: byte(millis >> 8), 1: byte(millis), 3: 2}
}

func requestedTimeParent(t *testing.T, w *worker, b *testWorkerBackend, millis uint64) *types.Block {
	t.Helper()
	result := w.generateWork(&generateParams{timestamp: millis / 1000, forceTime: true, random: requestedTimeRandom(millis % 1000), noTxs: true})
	if result.err != nil {
		t.Fatal(result.err)
	}
	if _, err := b.chain.InsertChain(types.Blocks{result.block}); err != nil {
		t.Fatal(err)
	}
	return result.block
}

func TestBundleBuilderRequestedBlockTime(t *testing.T) {
	for _, name := range []string{"same-second", "second-rollover", "explicit-parent"} {
		t.Run(name, func(t *testing.T) {
			m, b := newAdmissionMiner(t, nil)
			w := m.worker
			w.newpayloadTimeout = 650 * time.Millisecond
			parent, target := b.chain.Genesis(), uint64(250)
			if name == "second-rollover" {
				parent = requestedTimeParent(t, w, b, 750)
				target = 1000
			} else if name == "explicit-parent" {
				parent = requestedTimeParent(t, w, b, 250)
				requestedTimeParent(t, w, b, 750)
				target = 500
				if parent.Hash() == b.chain.CurrentBlock().Hash() {
					t.Fatal("test requires a parent different from CurrentBlock")
				}
			}
			params := &generateParams{timestamp: target / 1000, random: requestedTimeRandom(target % 1000), parentHash: parent.Hash(), forceTime: true}
			interval, err := w.validateParams(params)
			if err != nil {
				t.Fatal(err)
			}
			if interval != 250*time.Millisecond {
				t.Errorf("requested 250ms span with encoded byte 2: got %v", interval)
			}
			// Direct sealing callers do not go through validateParams/buildPayload.
			work, err := w.prepareWork(params)
			if err != nil {
				t.Fatal(err)
			}
			defer work.discard()
			start := time.Now()
			ctx, cancel := w.bundleBuildContext(context.Background(), start, params.blockTime)
			defer cancel()
			deadline, _ := ctx.Deadline()
			if got := deadline.Sub(start); got != 125*time.Millisecond {
				t.Errorf("direct sealing bundle allowance: got %v, want 125ms", got)
			}

			// Hold the real payload request at the worker queue so the absolute
			// context can be inspected without build speed affecting its lifetime.
			w.getWorkCh = make(chan *getWorkReq)
			w.exitCh = make(chan struct{})
			before := time.Now()
			payload, err := w.buildPayload(&BuildPayloadArgs{Parent: parent.Hash(), Timestamp: target / 1000, Random: requestedTimeRandom(target % 1000)})
			after := time.Now()
			if err != nil {
				t.Fatal(err)
			}
			defer payload.stopBuilding()
			deadline, _ = payload.ctx.Deadline()
			if deadline.Before(before.Add(250*time.Millisecond)) || deadline.After(after.Add(250*time.Millisecond)) {
				t.Errorf("payload lifetime: deadline offset %v..%v, want 250ms", deadline.Sub(after), deadline.Sub(before))
			}
			select {
			case req := <-w.getWorkCh:
				if req.params.ctx != payload.ctx {
					t.Error("generation lost absolute payload context")
				}
				payload.stopBuilding()
				req.result <- &newPayloadResult{err: context.Canceled}
			case <-time.After(3 * time.Second):
				t.Fatal("initial build never reached worker queue")
			}
		})
	}
}

func TestBundleBuilderEffectivePreparedInterval(t *testing.T) {
	m, b := newAdmissionMiner(t, nil)
	w := m.worker
	params := &generateParams{random: requestedTimeRandom(500), forceTime: true}
	interval, err := w.validateParams(params)
	if err != nil {
		t.Fatal(err)
	}
	params.blockTime = interval
	// A direct caller following CurrentBlock may resolve a newer parent during
	// preparation. A previously supplied interval must not survive that change.
	requestedTimeParent(t, w, b, 250)
	work, err := w.prepareWork(params)
	if err != nil {
		t.Fatal(err)
	}
	work.discard()
	if params.blockTime != 250*time.Millisecond {
		t.Fatalf("stale interval after parent advance: %v", params.blockTime)
	}
	params.random = requestedTimeRandom(250)
	if _, err := w.validateParams(params); err == nil {
		t.Fatal("forced timestamp equal to parent accepted by validation")
	}
	if _, err := w.prepareWork(params); err == nil {
		t.Fatal("forced timestamp equal to parent accepted by preparation")
	}
	params.forceTime = false
	interval, err = w.validateParams(params)
	if err != nil {
		t.Fatal(err)
	}
	work, err = w.prepareWork(params)
	if err != nil {
		t.Fatal(err)
	}
	defer work.discard()
	want := time.Duration(work.header.MilliTimestamp()-b.chain.CurrentBlock().MilliTimestamp()) * time.Millisecond
	if want <= 0 || interval != want || params.blockTime != want {
		t.Fatalf("corrected timestamp interval: validated %v, prepared %v, want %v", interval, params.blockTime, want)
	}
}

func TestBundleBuilderRequestedTimeRecommit(t *testing.T) {
	m, b := newAdmissionMiner(t, nil)
	w := m.worker
	w.newpayloadTimeout = 650 * time.Millisecond
	w.recommit = time.Millisecond
	w.getWorkCh = make(chan *getWorkReq)
	w.exitCh = make(chan struct{})
	// TestBundleBuilderRequestedBlockTime independently covers the real 250ms
	// request with byte 2 and its 125ms bundle budget. This lifecycle test uses
	// a generous span so recommits do not depend on real generation finishing
	// within 125ms on a loaded or race-instrumented host.
	payload, err := w.buildPayload(&BuildPayloadArgs{Parent: b.chain.CurrentBlock().Hash(), Timestamp: 60, Random: requestedTimeRandom(0), Transactions: genTxs(0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer payload.stopBuilding()
	deadline, _ := payload.ctx.Deadline()
	for generation := 0; generation < 2; generation++ {
		var req *getWorkReq
		select {
		case req = <-w.getWorkCh:
		case <-time.After(3 * time.Second):
			t.Fatal("generation never reached worker queue")
		}
		if req.params.blockTime != time.Minute || req.params.ctx != payload.ctx {
			t.Errorf("generation %d lost validated interval or payload context", generation)
		}
		gotDeadline, _ := req.params.ctx.Deadline()
		if !gotDeadline.Equal(deadline) {
			t.Error("recommit renewed absolute deadline")
		}
		if generation == 0 {
			req.result <- w.generateWork(req.params)
		} else {
			if !req.params.isUpdate {
				t.Error("second generation was not a recommit")
			}
			ctx, cancel := w.bundleBuildContext(req.params.ctx, deadline.Add(-10*time.Millisecond), req.params.blockTime)
			gotDeadline, _ = ctx.Deadline()
			cancel()
			if !gotDeadline.Equal(deadline) {
				t.Error("late recommit extended parent deadline")
			}
			payload.stopBuilding()
			req.result <- &newPayloadResult{err: context.Canceled}
		}
	}
}
