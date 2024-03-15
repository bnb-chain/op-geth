package legacypool

import (
	"math/big"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
)

// copy of pending transactions
type cacheForMiner struct {
	txLock             sync.Mutex
	pending            map[common.Address]map[*types.Transaction]struct{}
	locals             map[common.Address]bool
	pendingWithoutTips atomic.Value
	pendingWithTips    atomic.Value
	addrLock           sync.Mutex
}

func (pc *cacheForMiner) add(txs types.Transactions, signer types.Signer) {
	if len(txs) == 0 {
		return
	}
	pc.txLock.Lock()
	defer pc.txLock.Unlock()
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
		delete(slots, tx)
		if len(slots) == 0 {
			delete(pc.pending, addr)
		}
	}
}

func (pc *cacheForMiner) dump(pool txpool.LazyResolver, gasPrice, baseFee *big.Int) {
	pending := make(map[common.Address]types.Transactions)
	pc.txLock.Lock()
	for addr, txlist := range pc.pending {
		pending[addr] = make(types.Transactions, 0, len(txlist))
		for tx := range txlist {
			pending[addr] = append(pending[addr], tx)
		}
	}
	pc.txLock.Unlock()
	// sorted by nonce
	for addr := range pending {
		sort.Sort(types.TxByNonce(pending[addr]))
	}
	pendingWithTips := make(map[common.Address]types.Transactions)
	for addr, txs := range pending {
		// If the miner requests tip enforcement, cap the lists now
		if !pc.isLocal(addr) {
			for i, tx := range txs {
				if tx.EffectiveGasTipIntCmp(gasPrice, baseFee) < 0 {
					txs = txs[:i]
					break
				}
			}
		}
		if len(txs) > 0 {
			pendingWithTips[addr] = txs
		}
	}

	// convert into LazyTransaction
	lazyPendingWithTips, lazyPendingWithoutTips := make(map[common.Address][]*txpool.LazyTransaction), make(map[common.Address][]*txpool.LazyTransaction)
	for addr, txs := range pending {
		for i, tx := range txs {
			lazyTx := &txpool.LazyTransaction{
				Pool:      pool,
				Hash:      tx.Hash(),
				Tx:        tx,
				Time:      tx.Time(),
				GasFeeCap: tx.GasFeeCap(),
				GasTipCap: tx.GasTipCap(),
				Gas:       tx.Gas(),
				BlobGas:   tx.BlobGas(),
			}
			lazyPendingWithoutTips[addr] = append(lazyPendingWithoutTips[addr], lazyTx)
			if len(pendingWithTips[addr]) > i {
				lazyPendingWithTips[addr] = append(lazyPendingWithTips[addr], lazyTx)
			}
		}
	}

	// store pending
	pc.pendingWithTips.Store(lazyPendingWithTips)
	pc.pendingWithoutTips.Store(lazyPendingWithoutTips)
}

func (pc *cacheForMiner) pendingTxs(enforceTips bool) map[common.Address][]*txpool.LazyTransaction {
	if enforceTips {
		return pc.pendingWithTips.Load().(map[common.Address][]*txpool.LazyTransaction)
	} else {
		return pc.pendingWithoutTips.Load().(map[common.Address][]*txpool.LazyTransaction)
	}
}

func (pc *cacheForMiner) markLocal(addr common.Address) {
	pc.addrLock.Lock()
	defer pc.addrLock.Unlock()
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
