package legacypool

import (
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/holiman/uint256"
)

var (
	pendingCacheGauge = metrics.NewRegisteredGauge("txpool/legacypool/pending/cache", nil)
	localCacheGauge   = metrics.NewRegisteredGauge("txpool/legacypool/local/cache", nil)
)

// copy of pending transactions
type cacheForMiner struct {
	txLock   sync.Mutex
	pending  map[common.Address]map[*types.Transaction]struct{}
	locals   map[common.Address]bool
	addrLock sync.Mutex

	allCache      map[common.Address][]*txpool.LazyTransaction
	filteredCache map[common.Address][]*txpool.LazyTransaction
	cacheLock     sync.Mutex
}

func newCacheForMiner() *cacheForMiner {
	return &cacheForMiner{
		pending:       make(map[common.Address]map[*types.Transaction]struct{}),
		locals:        make(map[common.Address]bool),
		allCache:      make(map[common.Address][]*txpool.LazyTransaction),
		filteredCache: make(map[common.Address][]*txpool.LazyTransaction),
	}
}

func (pc *cacheForMiner) add(txs types.Transactions, signer types.Signer) {
	if len(txs) == 0 {
		return
	}
	pc.txLock.Lock()
	defer pc.txLock.Unlock()
	pendingCacheGauge.Inc(int64(len(txs)))
	for _, tx := range txs {
		addr, _ := types.Sender(signer, tx)
		slots, ok := pc.pending[addr]
		if !ok {
			slots = make(map[*types.Transaction]struct{})
			pc.pending[addr] = slots
		}
		slots[tx] = struct{}{}
	}
}

func (pc *cacheForMiner) del(txs types.Transactions, signer types.Signer) {
	if len(txs) == 0 {
		return
	}
	pc.txLock.Lock()
	defer pc.txLock.Unlock()
	for _, tx := range txs {
		addr, _ := types.Sender(signer, tx)
		slots, ok := pc.pending[addr]
		if !ok {
			continue
		}
		pendingCacheGauge.Dec(1)
		delete(slots, tx)
		if len(slots) == 0 {
			delete(pc.pending, addr)
		}
	}
}

func (pc *cacheForMiner) sync2cache(pool txpool.LazyResolver, filter func(txs types.Transactions, addr common.Address) types.Transactions) {
	pending := make(map[common.Address]types.Transactions)

	pc.txLock.Lock()
	for addr, txlist := range pc.pending {
		pending[addr] = make(types.Transactions, 0, len(txlist))
		for tx := range txlist {
			pending[addr] = append(pending[addr], tx)
		}
	}
	pc.txLock.Unlock()

	// convert pending to lazyTransactions
	filteredLazy := make(map[common.Address][]*txpool.LazyTransaction)
	allLazy := make(map[common.Address][]*txpool.LazyTransaction)
	for addr, txs := range pending {
		// sorted by nonce
		sort.Sort(types.TxByNonce(txs))
		filterd := filter(txs, addr)
		if len(txs) > 0 {
			lazies := make([]*txpool.LazyTransaction, len(txs))
			for i, tx := range txs {
				lazies[i] = &txpool.LazyTransaction{
					Pool:      pool,
					Hash:      tx.Hash(),
					Tx:        tx,
					Time:      tx.Time(),
					GasFeeCap: uint256.MustFromBig(tx.GasFeeCap()),
					GasTipCap: uint256.MustFromBig(tx.GasTipCap()),
					Gas:       tx.Gas(),
					BlobGas:   tx.BlobGas(),
				}
			}
			allLazy[addr] = lazies
			filteredLazy[addr] = lazies[:len(filterd)]
		}
	}

	pc.cacheLock.Lock()
	pc.filteredCache = filteredLazy
	pc.allCache = allLazy
	pc.cacheLock.Unlock()
}

func (pc *cacheForMiner) dump(filtered bool) map[common.Address][]*txpool.LazyTransaction {
	pc.cacheLock.Lock()
	pending := pc.allCache
	if filtered {
		pending = pc.filteredCache
	}
	pc.cacheLock.Unlock()
	return pending
}

func (pc *cacheForMiner) markLocal(addr common.Address) {
	pc.addrLock.Lock()
	defer pc.addrLock.Unlock()
	localCacheGauge.Inc(1)
	pc.locals[addr] = true
}

func (pc *cacheForMiner) IsLocal(addr common.Address) bool {
	pc.addrLock.Lock()
	defer pc.addrLock.Unlock()
	return pc.locals[addr]
}

func (pc *cacheForMiner) flattenLocals() []common.Address {
	pc.addrLock.Lock()
	defer pc.addrLock.Unlock()
	locals := make([]common.Address, 0, len(pc.locals))
	for addr := range pc.locals {
		locals = append(locals, addr)
	}
	return locals
}
