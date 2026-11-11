package miner

import (
	"errors"
	"math/big"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// Leave room for block and batch framing below both the 10 MB channel limit
// and the 10 MiB decoded gossip limit.
const testPayloadSizeBudget = 8*1024*1024 - 1_000_000

func newPayloadSizeWorker(t *testing.T, optimism, mev bool) (*worker, *testWorkerBackend, types.Transactions) {
	t.Helper()
	cfg := *testConfig
	cfg.GasCeil = 100_000_000
	cfg.NewPayloadTimeout = 10 * time.Second
	cfg.Mev.MevEnabled = mev
	chainConfig := *params.TestChainConfig
	if optimism {
		chainConfig.Optimism = &params.OptimismConfig{
			EIP1559Elasticity: 2, EIP1559Denominator: 8, EIP1559DenominatorCanyon: 8,
		}
	}
	w, backend := newTestWorker(t, &chainConfig, ethash.NewFaker(), rawdb.NewMemoryDatabase(), 0, &cfg, nil)
	t.Cleanup(w.close)
	txs := make(types.Transactions, 100)
	for i := range txs {
		txs[i] = types.MustSignNewTx(testBankKey, types.LatestSigner(&chainConfig), &types.LegacyTx{
			Nonce: uint64(i), To: &testUserAddress,
			Gas: params.TxGas + params.TxDataZeroGas*130_000, GasPrice: big.NewInt(2 * params.InitialBaseFee),
			Data: make([]byte, 130_000),
		})
		if txs[i].Size() > 128*1024 {
			t.Fatal("reproducer transaction exceeds txpool size limit")
		}
	}
	return w, backend, txs
}

func payloadSizeParams(backend *testWorkerBackend) *generateParams {
	gasLimit := uint64(100_000_000)
	return &generateParams{
		parentHash: backend.chain.CurrentBlock().Hash(), timestamp: uint64(time.Now().Unix()),
		forceTime: true, coinbase: testBankAddress, gasLimit: &gasLimit,
	}
}

func assertPayloadSize(t *testing.T, block *types.Block) {
	t.Helper()
	var size uint64
	for _, tx := range block.Transactions() {
		size += tx.Size()
	}
	if size > testPayloadSizeBudget {
		t.Fatalf("oversized block produced: txs=%d tx_bytes=%d budget=%d", len(block.Transactions()), size, testPayloadSizeBudget)
	}
	if len(block.Transactions()) == 0 || len(block.Transactions()) >= 100 {
		t.Fatalf("expected a nonempty partial block, got %d transactions", len(block.Transactions()))
	}
	if block.GasUsed() > block.GasLimit() {
		t.Fatal("block exceeds gas limit")
	}
	t.Logf("txs=%d tx_bytes=%d block_bytes=%d gas_used=%d", len(block.Transactions()), size, block.Size(), block.GasUsed())
}

func TestPayloadSizeTxPool(t *testing.T) {
	for _, mev := range []bool{false, true} {
		t.Run(map[bool]string{false: "txpool", true: "mev-fallback-with-forced-prefix"}[mev], func(t *testing.T) {
			w, backend, txs := newPayloadSizeWorker(t, true, mev)
			for i, err := range backend.txPool.Add(txs, true, true) {
				if err != nil {
					t.Fatalf("add tx %d: %v", i, err)
				}
			}
			args := payloadSizeParams(backend)
			if mev {
				args.txs = txs[:10]
			}
			result := w.generateWork(args)
			if result.err != nil {
				t.Fatal(result.err)
			}
			assertPayloadSize(t, result.block)
			for i, tx := range result.block.Transactions() {
				if tx.Hash() != txs[i].Hash() {
					t.Fatalf("transaction %d was lost or reordered", i)
				}
			}
		})
	}
}

func TestPayloadSizeForcedTransactions(t *testing.T) {
	w, backend, txs := newPayloadSizeWorker(t, true, false)
	args := payloadSizeParams(backend)
	args.txs = txs
	if result := w.generateWork(args); !errors.Is(result.err, errBlockSizeLimitReached) {
		t.Fatalf("expected size limit error, got %v", result.err)
	}
	// Forced reconstruction is used for historical derivation. Preserve its
	// semantics instead of introducing an unannounced consensus rule.
	args.noTxs = true
	result := w.generateWork(args)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.block.Transactions()) != len(txs) {
		t.Fatal("historical reconstruction truncated forced transactions")
	}
	gasLimit := uint64(100_000_000)
	payloadArgs := &BuildPayloadArgs{
		Parent: backend.chain.CurrentBlock().Hash(), Timestamp: args.timestamp,
		FeeRecipient: common.Address{}, Transactions: txs, GasLimit: &gasLimit,
	}
	if _, err := w.buildPayload(payloadArgs); !errors.Is(err, errBlockSizeLimitReached) {
		t.Fatalf("expected synchronous size limit error, got %v", err)
	}
	payloadArgs.NoTxPool = true
	payload, err := w.buildPayload(payloadArgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Resolve().ExecutionPayload.Transactions) != len(txs) {
		t.Fatal("NoTxPool payload truncated historical transactions")
	}
}

func TestPayloadSizeBundles(t *testing.T) {
	w, backend, txs := newPayloadSizeWorker(t, true, false)
	env, err := w.prepareWork(payloadSizeParams(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	env.gasPool = new(core.GasPool).AddGas(env.header.GasLimit)
	env.UnRevertible = mapset.NewSet[common.Hash]()
	if err := w.commitBundles(env, txs, nil); err == nil {
		t.Fatal("accepted oversized bundle")
	}
	if len(env.txs) == 0 {
		t.Fatal("bundle failed before including any transactions")
	}
	var size uint64
	for _, tx := range env.txs {
		size += tx.Size()
	}
	if size > testPayloadSizeBudget {
		t.Fatalf("bundle bypassed byte budget: %d", size)
	}
}

func TestPayloadSizeAccounting(t *testing.T) {
	w, backend, txs := newPayloadSizeWorker(t, true, false)
	env, err := w.prepareWork(payloadSizeParams(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer env.discard()
	env.gasPool = new(core.GasPool).AddGas(env.header.GasLimit)
	// A failed transaction must not consume the byte budget.
	if _, err := w.commitTransaction(env, txs[1]); !errors.Is(err, core.ErrNonceTooHigh) {
		t.Fatalf("expected nonce error, got %v", err)
	}
	if env.size != 0 {
		t.Fatal("failed transaction consumed byte budget")
	}
	// Inclusion exactly at the boundary is permitted.
	env.sizeLimit = txs[0].Size()
	if _, err := w.commitTransaction(env, txs[0]); err != nil {
		t.Fatal(err)
	}
	copy := env.copy()
	defer copy.discard()
	if copy.size != env.size || copy.sizeLimit != env.sizeLimit {
		t.Fatal("environment copy lost byte accounting")
	}
	for name, candidate := range map[string]*environment{"original": env, "copy": copy} {
		root := candidate.state.IntermediateRoot(true)
		gas := candidate.gasPool.Gas()
		used := candidate.header.GasUsed
		size := candidate.size
		if _, err := w.commitTransaction(candidate, txs[1]); !errors.Is(err, errBlockSizeLimitReached) {
			t.Fatalf("%s: expected size error, got %v", name, err)
		}
		if candidate.state.IntermediateRoot(true) != root || candidate.gasPool.Gas() != gas || candidate.header.GasUsed != used || candidate.size != size || len(candidate.txs) != 1 {
			t.Fatalf("%s: size rejection mutated the environment", name)
		}
	}
}

func TestPayloadSizePolicyScope(t *testing.T) {
	// Non-rollup production must retain the existing gas-only behavior.
	w, backend, txs := newPayloadSizeWorker(t, false, false)
	args := payloadSizeParams(backend)
	args.txs = txs
	result := w.generateWork(args)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.block.Transactions()) != len(txs) {
		t.Fatal("rollup size policy affected non-rollup production")
	}
}
