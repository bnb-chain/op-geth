// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package legacypool

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

func TestListAddL1Cost(t *testing.T) {
	for _, tc := range []struct {
		name   string
		costFn txpool.L1CostFunc
		fee    int64
	}{
		{name: "no estimator"},
		{name: "nil estimate", costFn: func(types.RollupCostData) *big.Int { return nil }},
		{name: "zero estimate", costFn: func(types.RollupCostData) *big.Int { return new(big.Int) }},
		{name: "low estimate", costFn: func(types.RollupCostData) *big.Int { return big.NewInt(100) }, fee: 100},
		{name: "high estimate", costFn: func(types.RollupCostData) *big.Int { return big.NewInt(999999999) }, fee: 999999999},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := types.NewTransaction(0, common.Address{}, big.NewInt(100), 21000, big.NewInt(1), nil)
			list := newRollupList(true, &LegacyPool{l1CostFn: tc.costFn})
			if ok, _ := list.Add(tx, DefaultConfig.PriceBump); !ok {
				t.Fatal("transaction rejected")
			}
			want := new(big.Int).Add(tx.Cost(), big.NewInt(tc.fee))
			if have := list.totalcost.ToBig(); have.Cmp(want) != 0 {
				t.Fatalf("total cost = %v, want %v", have, want)
			}
			list.Remove(tx)
			if !list.totalcost.IsZero() {
				t.Fatalf("cost after removal = %v, want 0", list.totalcost)
			}
		})
	}
}

func TestListRemovalL1Cost(t *testing.T) {
	for _, tc := range []struct {
		name      string
		strict    bool
		remove    func(*list, types.Transactions)
		remaining []int
	}{
		{"forward", true, func(l *list, _ types.Transactions) { l.Forward(1) }, []int{1, 2}},
		{"cap", true, func(l *list, _ types.Transactions) { l.Cap(2) }, []int{0, 1}},
		{"ready", true, func(l *list, _ types.Transactions) { l.Ready(0) }, nil},
		{"remove strict", true, func(l *list, txs types.Transactions) { l.Remove(txs[1]) }, []int{0}},
		{"remove nonstrict", false, func(l *list, txs types.Transactions) { l.Remove(txs[1]) }, []int{0, 2}},
		{"filter strict", true, func(l *list, _ types.Transactions) { l.Filter(uint256.NewInt(1000000000), 25000) }, []int{0}},
		{"filter nonstrict", false, func(l *list, _ types.Transactions) { l.Filter(uint256.NewInt(1000000000), 25000) }, []int{0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fee := big.NewInt(100)
			costFn := func(types.RollupCostData) *big.Int { return fee }
			list := newRollupList(tc.strict, &LegacyPool{l1CostFn: costFn})
			txs := make(types.Transactions, 3)
			for i, gas := range []uint64{21000, 30000, 21000} {
				txs[i] = types.NewTransaction(uint64(i), common.Address{}, big.NewInt(100), gas, big.NewInt(1), nil)
				if ok, _ := list.Add(txs[i], DefaultConfig.PriceBump); !ok {
					t.Fatal("transaction rejected")
				}
			}
			// The estimator may change or reuse its result after admission.
			fee.SetInt64(999999)
			tc.remove(list, txs)
			want := new(big.Int)
			for _, i := range tc.remaining {
				want.Add(want, new(big.Int).Add(txs[i].Cost(), big.NewInt(100)))
			}
			if list.Len() != len(tc.remaining) {
				t.Fatalf("length = %d, want %d", list.Len(), len(tc.remaining))
			}
			if have := list.totalcost.ToBig(); have.Cmp(want) != 0 {
				t.Fatalf("remaining cost = %v, want %v", have, want)
			}
			list.Cap(0)
			if !list.totalcost.IsZero() {
				t.Fatalf("cost after clearing = %v, want 0", list.totalcost)
			}
		})
	}
}

func TestListReplacementL1Cost(t *testing.T) {
	tx := types.NewTransaction(0, common.Address{}, big.NewInt(100), 21000, big.NewInt(1), nil)
	replacement := types.NewTransaction(0, common.Address{}, big.NewInt(100), 21000, big.NewInt(2), nil)
	fee := big.NewInt(100000)
	costFn := func(types.RollupCostData) *big.Int { return fee }
	list := newRollupList(true, &LegacyPool{l1CostFn: costFn})
	list.Add(tx, DefaultConfig.PriceBump)
	fee.SetInt64(100)
	before := new(uint256.Int).Set(list.totalcost)
	if ok, _ := list.Add(tx, DefaultConfig.PriceBump); ok {
		t.Fatal("underpriced replacement accepted")
	}
	if !list.totalcost.Eq(before) {
		t.Fatal("rejected replacement changed cost")
	}
	if ok, old := list.Add(replacement, DefaultConfig.PriceBump); !ok || old != tx {
		t.Fatal("replacement failed")
	}
	want := new(big.Int).Add(replacement.Cost(), fee)
	if have := list.totalcost.ToBig(); have.Cmp(want) != 0 {
		t.Fatalf("replacement cost = %v, want %v", have, want)
	}
	list.Remove(replacement)
	if !list.totalcost.IsZero() {
		t.Fatalf("cost after removal = %v, want 0", list.totalcost)
	}
}

func TestListCostOverflow(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	for _, tc := range []struct {
		name         string
		initialValue *big.Int
		nonce        uint64
		value        *big.Int
		fee          *big.Int
	}{
		{"L2 replacement", big.NewInt(100), 0, max, nil},
		{"L1 conversion", big.NewInt(100), 1, big.NewInt(0), new(big.Int).Add(max, big.NewInt(1))},
		{"L1 sum", big.NewInt(100), 1, big.NewInt(0), max},
		{"aggregate", new(big.Int).Sub(max, big.NewInt(10)), 1, big.NewInt(100), nil},
		{"replacement aggregate", new(big.Int).Sub(max, big.NewInt(10)), 0, new(big.Int).Sub(max, big.NewInt(3)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := new(LegacyPool)
			list := newRollupList(true, pool)
			initial := types.NewTransaction(0, common.Address{}, tc.initialValue, 1, big.NewInt(1), nil)
			if ok, _ := list.Add(initial, DefaultConfig.PriceBump); !ok {
				t.Fatal("initial transaction rejected")
			}
			// Keep another reservation to exercise replacement aggregate overflow.
			if tc.name == "replacement aggregate" {
				list.Add(types.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), nil), DefaultConfig.PriceBump)
			}
			before := new(uint256.Int).Set(list.totalcost)
			costcap, gascap, length := new(uint256.Int).Set(list.costcap), list.gascap, list.Len()
			candidate := types.NewTransaction(tc.nonce, common.Address{}, tc.value, 1, big.NewInt(2), nil)
			pool.l1CostFn = func(types.RollupCostData) *big.Int { return tc.fee }
			if ok, old := list.Add(candidate, DefaultConfig.PriceBump); ok || old != nil {
				t.Fatal("overflowing transaction accepted")
			}
			if !list.totalcost.Eq(before) || !list.costcap.Eq(costcap) || list.gascap != gascap || list.Len() != length || list.txs.Get(0) != initial {
				t.Fatal("rejected insertion changed list state")
			}
			list.Cap(0)
			if !list.totalcost.IsZero() {
				t.Fatalf("cost after clearing = %v, want 0", list.totalcost)
			}
		})
	}
}

func TestListCostUint256Boundary(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	list := newList(true)
	value := new(big.Int).Sub(max, big.NewInt(2))
	tx := types.NewTransaction(0, common.Address{}, value, 1, big.NewInt(1), nil)
	replacement := types.NewTransaction(0, common.Address{}, value, 1, big.NewInt(2), nil)
	if ok, _ := list.Add(tx, DefaultConfig.PriceBump); !ok {
		t.Fatal("initial transaction rejected")
	}
	if ok, old := list.Add(replacement, DefaultConfig.PriceBump); !ok || old != tx {
		t.Fatal("valid boundary replacement rejected")
	}
	if list.totalcost.ToBig().Cmp(max) != 0 {
		t.Fatalf("cost = %v, want %v", list.totalcost, max)
	}
	list.Remove(replacement)
	if !list.totalcost.IsZero() {
		t.Fatalf("cost after removal = %v, want 0", list.totalcost)
	}
	// The L1-inclusive reservation may also equal the maximum exactly.
	l1Fee := new(big.Int).Sub(max, big.NewInt(1))
	list = newRollupList(true, &LegacyPool{l1CostFn: func(types.RollupCostData) *big.Int { return l1Fee }})
	tx = types.NewTransaction(0, common.Address{}, new(big.Int), 1, big.NewInt(1), nil)
	if ok, _ := list.Add(tx, DefaultConfig.PriceBump); !ok {
		t.Fatal("valid L1-inclusive boundary cost rejected")
	}
	if list.totalcost.ToBig().Cmp(max) != 0 {
		t.Fatalf("L1-inclusive cost = %v, want %v", list.totalcost, max)
	}
	list.Remove(tx)
	if !list.totalcost.IsZero() {
		t.Fatalf("cost after removal = %v, want 0", list.totalcost)
	}
}

func TestLegacyPoolL1CostReset(t *testing.T) {
	config := *params.TestChainConfig
	config.Optimism = &params.OptimismConfig{}
	pool, key := setupPoolWithConfig(&config)
	defer pool.Close()
	sender := crypto.PubkeyToAddress(key.PublicKey)
	txs := make(types.Transactions, 4)
	for i := range txs {
		txs[i] = transaction(uint64(i), 21000, key)
	}
	// Use the real L1 estimator, backed by the pool's current L1Block state.
	pool.mu.Lock()
	pool.currentState.SetBalance(sender, uint256.NewInt(1000000000))
	pool.currentState.SetState(types.L1BlockAddr, types.L1BaseFeeSlot, common.BigToHash(big.NewInt(1)))
	pool.currentState.SetState(types.L1BlockAddr, types.ScalarSlot, common.BigToHash(big.NewInt(1000000)))
	pool.reset(nil, nil)
	oldFee := new(big.Int).Set(pool.l1CostFn(txs[1].RollupCostData()))
	pool.mu.Unlock()
	for _, err := range pool.addRemotesSync([]*types.Transaction{txs[0], txs[1], txs[3]}) {
		if err != nil {
			t.Fatalf("initial admission failed: %v", err)
		}
	}
	// A reset installs a new estimator and removes mined nonces. Surviving
	// transactions keep their original reservation until they move or replace.
	pool.mu.Lock()
	pool.currentState.SetNonce(sender, 1)
	pool.currentState.SetState(types.L1BlockAddr, types.L1BaseFeeSlot, common.BigToHash(big.NewInt(1000)))
	pool.mu.Unlock()
	<-pool.requestReset(nil, nil)
	pool.mu.Lock()
	want := new(big.Int).Add(txs[1].Cost(), oldFee)
	pending := pool.pending[sender]
	if pending == nil || pending.Len() != 1 || pending.totalcost.ToBig().Cmp(want) != 0 {
		pool.mu.Unlock()
		t.Fatalf("reset did not preserve the surviving reservation of %v", want)
	}
	newFee := new(big.Int).Set(pool.l1CostFn(txs[1].RollupCostData()))
	pool.mu.Unlock()
	if newFee.Cmp(oldFee) <= 0 {
		t.Fatal("reset did not update L1 fee estimator")
	}
	// Filling the nonce gap promotes tx3 from queue to pending, charging the
	// current estimate while removing its original queue reservation.
	if err := pool.addRemotesSync([]*types.Transaction{txs[2]})[0]; err != nil {
		t.Fatalf("gap-filling admission failed: %v", err)
	}
	pool.mu.Lock()
	for _, tx := range txs[2:] {
		want.Add(want, new(big.Int).Add(tx.Cost(), pool.l1CostFn(tx.RollupCostData())))
	}
	pending = pool.pending[sender]
	if pending == nil || pending.Len() != 3 || pending.totalcost.ToBig().Cmp(want) != 0 || pool.queue[sender] != nil {
		pool.mu.Unlock()
		t.Fatalf("promotion did not reserve expected cost %v", want)
	}
	pool.currentState.SetNonce(sender, 4)
	pool.mu.Unlock()
	<-pool.requestReset(nil, nil)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.pending[sender] != nil || !pending.totalcost.IsZero() {
		t.Fatal("reset left a reservation for mined transactions")
	}
}

func TestLegacyPoolL1CostSolvency(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		oldFee, newFee, balance int64
		replace, insufficient   bool
	}{
		{name: "affordable", oldFee: 100, newFee: 100, balance: 50000},
		{name: "unaffordable", oldFee: 100000, newFee: 100000, balance: 200000, insufficient: true},
		{name: "replacement fee increase", oldFee: 100, newFee: 100000, balance: 160000, replace: true, insufficient: true},
		{name: "replacement fee decrease", oldFee: 100000, newFee: 100, balance: 170000, replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, key := setupPool()
			defer pool.Close()
			pool.mu.Lock()
			defer pool.mu.Unlock()
			sender := crypto.PubkeyToAddress(key.PublicKey)
			pool.currentState.SetBalance(sender, uint256.NewInt(uint64(tc.balance)))
			pool.l1CostFn = func(types.RollupCostData) *big.Int { return big.NewInt(tc.oldFee) }
			list := newRollupList(true, pool)
			list.Add(transaction(0, 21000, key), DefaultConfig.PriceBump)
			if tc.replace {
				list.Add(transaction(1, 21000, key), DefaultConfig.PriceBump)
			}
			pool.pending[sender] = list
			pool.l1CostFn = func(types.RollupCostData) *big.Int { return big.NewInt(tc.newFee) }
			candidate := transaction(1, 21000, key)
			if tc.replace {
				candidate = pricedTransaction(0, 21000, big.NewInt(2), key)
			}
			err := pool.validateTx(candidate, false)
			if tc.insufficient {
				if !errors.Is(err, core.ErrInsufficientFunds) {
					t.Fatalf("validation error = %v, want insufficient funds", err)
				}
			} else if err != nil {
				t.Fatalf("affordable transaction rejected: %v", err)
			}
		})
	}
}
