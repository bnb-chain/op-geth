package bundlepool

import (
	"container/heap"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/miner"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"testing"
)

type testBlockChain struct {
	config   *params.ChainConfig
	gasLimit uint64
	statedb  *state.StateDB
}

func (bc *testBlockChain) Config() *params.ChainConfig {
	return bc.config
}

func (bc *testBlockChain) CurrentBlock() *types.Header {
	return &types.Header{
		Number: new(big.Int),
	}
}

func (bc *testBlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	return nil
}

func (bc *testBlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return bc.statedb, nil
}

func setupBundlePool(config Config, mevConfig miner.MevConfig) *BundlePool {
	return setupBundlePoolWithConfig(config, mevConfig)
}

func setupBundlePoolWithConfig(config Config, mevConfig miner.MevConfig) *BundlePool {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
	blockchain := newTestBlockChain(params.TestChainConfig, 100000000, statedb)

	pool := New(config, mevConfig, blockchain)
	return pool
}

func newTestBlockChain(config *params.ChainConfig, gasLimit uint64, statedb *state.StateDB) *testBlockChain {
	bc := testBlockChain{config: config, gasLimit: gasLimit, statedb: statedb}
	return &bc
}

func TestBundlePoolDrop(t *testing.T) {
	bundleConfig := Config{GlobalSlots: 5}
	mevConfig := miner.MevConfig{MevEnabled: true}
	bundlepool := setupBundlePool(bundleConfig, mevConfig)
	for i := 0; i < 5; i++ {
		bundle := &types.Bundle{
			Price: big.NewInt(int64(i)),
		}
		hash := bundle.Hash()
		bundlepool.bundles[hash] = bundle
		heap.Push(&bundlepool.bundleHeap, bundle)
		bundlepool.slots += numSlots(bundle)
	}

	// now bundle pool order by price [0, 1, 2, 3, 4]
	// test drop, one bundle with one slot replace success
	bundle := &types.Bundle{
		Txs:   nil,
		Price: big.NewInt(5),
	}
	if !bundlepool.drop(bundle) {
		t.Errorf("bundle pool drop expect success, but failed")
	}
	// check old least price bundle (bundle price is 0) not exist in bundle pool
	leastPriceBundleHash := heap.Pop(&bundlepool.bundleHeap).(*types.Bundle).Hash()
	if leastPriceBundle, ok := bundlepool.bundles[leastPriceBundleHash]; ok {
		if leastPriceBundle.Price.Uint64() == 0 {
			t.Errorf("old bundle expect not in bundlepool, but in")
		}
		heap.Push(&bundlepool.bundleHeap, leastPriceBundle)
	}
	hash := bundle.Hash()
	bundlepool.bundles[hash] = bundle
	heap.Push(&bundlepool.bundleHeap, bundle)
	bundlepool.slots += numSlots(bundle)

	// now bundle pool as price [1, 2, 3, 4, 5]
	// test drop, one bundle with 2 slot replace failed
	data := make([]byte, bundleSlotSize+1)
	for i := range data {
		data[i] = 0xFF
	}
	txs := types.Transactions{
		types.NewTx(&types.LegacyTx{
			Data: data,
		}),
	}
	bundle = &types.Bundle{
		Txs:   txs,
		Price: big.NewInt(2),
	}
	if bundlepool.drop(bundle) {
		t.Errorf("bundle pool drop expect failed, but success")
	}
	// check old least price bundle (bundle price is 1) exist in bundle pool, not dropped
	leastPriceBundleHash = heap.Pop(&bundlepool.bundleHeap).(*types.Bundle).Hash()
	if leastPriceBundle, ok := bundlepool.bundles[leastPriceBundleHash]; ok {
		if leastPriceBundle.Price.Uint64() != 1 {
			t.Errorf("old bundle expect in bundlepool, but dropped")
		}
		heap.Push(&bundlepool.bundleHeap, leastPriceBundle)
	}

	// now bundle pool as price  [1, 2, 3, 4, 5]
	// test drop, one bundle with 2 slot replace success
	bundle = &types.Bundle{
		Txs:   txs,
		Price: big.NewInt(3),
	}
	if !bundlepool.drop(bundle) {
		t.Errorf("bundle pool drop expect success, but failed")
	}
	// check old least bundle (bundle price is 1 and 2) not exist in bundle pool, dropped
	leastPriceBundleHash = heap.Pop(&bundlepool.bundleHeap).(*types.Bundle).Hash()
	if leastPriceBundle, ok := bundlepool.bundles[leastPriceBundleHash]; ok {
		if leastPriceBundle.Price.Uint64() != 3 {
			t.Errorf("old bundle expect not in bundlepool, but in")
		}
		heap.Push(&bundlepool.bundleHeap, leastPriceBundle)
	}
}
