package legacypool

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	_ pendingCache = (*noneCacheForMiner)(nil)
)

type noneCacheForMiner struct {
	pool *LegacyPool
}

func newNoneCacheForMiner(pool *LegacyPool) *noneCacheForMiner {
	return &noneCacheForMiner{pool: pool}
}

func (nc *noneCacheForMiner) add(txs types.Transactions, signer types.Signer) {
	// do nothing
}

func (nc *noneCacheForMiner) del(txs types.Transactions, signer types.Signer) {
	// do nothing
}

func (nc *noneCacheForMiner) dump() (map[common.Address]types.Transactions, map[common.Address]bool) {
	// dump all pending transactions from the pool
	nc.pool.mu.RLock()
	defer nc.pool.mu.RUnlock()
	pending := make(map[common.Address]types.Transactions)
	for addr, txlist := range nc.pool.pending {
		pending[addr] = txlist.Flatten()
	}
	locals := make(map[common.Address]bool, len(nc.pool.locals.accounts))
	for addr := range nc.pool.locals.accounts {
		locals[addr] = true
	}
	return pending, locals
}

func (nc *noneCacheForMiner) markLocal(addr common.Address) {
	// do nothing
}

func (nc *noneCacheForMiner) flattenLocals() []common.Address {
	// return a copy of pool.locals
	nc.pool.mu.RLock()
	defer nc.pool.mu.RUnlock()
	var locals []common.Address = make([]common.Address, 0, len(nc.pool.locals.accounts))
	for addr := range nc.pool.locals.accounts {
		locals = append(locals, addr)
	}
	return locals
}
