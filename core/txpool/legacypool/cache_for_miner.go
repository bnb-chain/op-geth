package legacypool

import (
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	pendingCacheGauge = metrics.NewRegisteredGauge("txpool/legacypool/pending/cache", nil)
	localCacheGauge   = metrics.NewRegisteredGauge("txpool/legacypool/local/cache", nil)
)

type pendingCache interface {
	add(types.Transactions, types.Signer)
	del(types.Transactions, types.Signer)
	dump() (map[common.Address]types.Transactions, map[common.Address]bool)
	markLocal(common.Address)
	flattenLocals() []common.Address
}

// copy of pending transactions
type cacheForMiner struct {
	txLock   sync.Mutex
	pending  map[common.Address]map[*types.Transaction]struct{}
	locals   map[common.Address]bool
	addrLock sync.Mutex
}

func newCacheForMiner() *cacheForMiner {
	return &cacheForMiner{
		pending: make(map[common.Address]map[*types.Transaction]struct{}),
		locals:  make(map[common.Address]bool),
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

func (pc *cacheForMiner) dump() (map[common.Address]types.Transactions, map[common.Address]bool) {
	pending := make(map[common.Address]types.Transactions)
	locals := make(map[common.Address]bool, len(pc.locals))
	pc.txLock.Lock()
	for addr, txlist := range pc.pending {
		pending[addr] = make(types.Transactions, 0, len(txlist))
		for tx := range txlist {
			pending[addr] = append(pending[addr], tx)
		}
	}
	pc.txLock.Unlock()
	pc.addrLock.Lock()
	for addr := range pc.locals {
		locals[addr] = true
	}
	pc.addrLock.Unlock()
	for _, txs := range pending {
		// sorted by nonce
		sort.Sort(types.TxByNonce(txs))
	}
	return pending, locals
}

func (pc *cacheForMiner) markLocal(addr common.Address) {
	pc.addrLock.Lock()
	defer pc.addrLock.Unlock()
	localCacheGauge.Inc(1)
	pc.locals[addr] = true
}

func (pc *cacheForMiner) isLocal(addr common.Address) bool {
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
