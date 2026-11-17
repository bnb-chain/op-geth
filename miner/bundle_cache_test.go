package miner

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"testing"
)

func TestBundleCacheBounded(t *testing.T) {
	cache := newCacheEntry(common.Hash{1})
	var first, last *types.Bundle
	for i := 0; i < 129; i++ {
		b := &types.Bundle{MaxBlockNumber: uint64(i + 1)}
		if i == 0 {
			first = b
		}
		last = b
		cache.UpdateSimulatedBundles(nil, []*types.Bundle{b})
	}
	if _, ok := cache.GetSimulatedBundle(first.Hash()); ok {
		t.Fatal("old failed simulation was not evicted")
	}
	if result, ok := cache.GetSimulatedBundle(last.Hash()); !ok || result != nil {
		t.Fatal("negative result was not cached")
	}
}
