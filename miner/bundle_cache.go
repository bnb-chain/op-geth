package miner

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	maxHeaders       = 3
	maxCachedBundles = 128
)

type BundleCache struct {
	mu      sync.Mutex
	entries []*BundleCacheEntry
}

func NewBundleCache() *BundleCache {
	return &BundleCache{
		entries: make([]*BundleCacheEntry, maxHeaders),
	}
}

func (b *BundleCache) GetBundleCache(header common.Hash) *BundleCacheEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, entry := range b.entries {
		if entry != nil && entry.headerHash == header {
			return entry
		}
	}
	newEntry := newCacheEntry(header)
	b.entries = b.entries[1:]
	b.entries = append(b.entries, newEntry)

	return newEntry
}

// BundleCacheEntry bounds both positive and negative results for one header.
// A cached nil result denotes a failed simulation.
type BundleCacheEntry struct {
	headerHash common.Hash
	results    *lru.Cache[common.Hash, *types.SimulatedBundle]
}

func newCacheEntry(header common.Hash) *BundleCacheEntry {
	return &BundleCacheEntry{headerHash: header, results: lru.NewCache[common.Hash, *types.SimulatedBundle](maxCachedBundles)}
}

func (c *BundleCacheEntry) GetSimulatedBundle(bundle common.Hash) (*types.SimulatedBundle, bool) {
	return c.results.Get(bundle)
}

func (c *BundleCacheEntry) UpdateSimulatedBundles(result map[common.Hash]*types.SimulatedBundle, bundles []*types.Bundle) {
	for _, bundle := range bundles {
		if bundle != nil {
			c.results.Add(bundle.Hash(), result[bundle.Hash()])
		}
	}
}
