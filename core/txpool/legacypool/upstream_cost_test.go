// Copyright 2026 The op-geth Authors
// This file is part of the op-geth library.
//
// The op-geth library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The op-geth library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the op-geth library. If not, see <http://www.gnu.org/licenses/>.

package legacypool

import (
	"math/big"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

func TestLegacyPoolL1CostFilter(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, expensiveFirst := range []bool{false, true} {
			name := "queue"
			if pending {
				name = "pending"
			}
			if expensiveFirst {
				name += "/expensive-first"
			} else {
				name += "/cheap-first"
			}
			t.Run(name, func(t *testing.T) {
				pool, key := setupPool()
				defer pool.Close()
				from := crypto.PubkeyToAddress(key.PublicKey)
				nonce := uint64(1)
				if pending {
					nonce = 0
				}
				txs := types.Transactions{
					pricedDataTransaction(nonce, 50000, big.NewInt(1), key, 0),
					pricedDataTransaction(nonce+1, 50000, big.NewInt(1), key, 64),
				}
				expensive := 1
				if expensiveFirst {
					expensive = 0
				}
				pool.mu.Lock()
				pool.currentState.SetBalance(from, uint256.NewInt(1000000))
				pool.l1CostFn = func(data types.RollupCostData) *big.Int {
					if data == txs[expensive].RollupCostData() {
						return big.NewInt(100000)
					}
					return big.NewInt(100)
				}
				pool.mu.Unlock()
				for _, err := range pool.addRemotesSync(txs) {
					if err != nil {
						t.Fatalf("initial admission failed: %v", err)
					}
				}
				pool.mu.Lock()
				defer pool.mu.Unlock()
				// Both L2 costs fit. Only the transaction with the low L1 fee fits
				// when its own complete cost is checked against the new balance.
				pool.currentState.SetBalance(from, uint256.NewInt(100000))
				if pending {
					pool.demoteUnexecutables(nil)
				} else {
					pool.promoteExecutables([]common.Address{from})
				}
				if pool.all.Get(txs[expensive].Hash()) != nil {
					t.Error("transaction with unaffordable L1 fee retained")
				}
				if pool.all.Get(txs[1-expensive].Hash()) == nil {
					t.Error("affordable transaction removed because of another transaction's L1 fee")
				}
			})
		}
	}
}

func TestLegacyPoolL1CostJournal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		journal  bool
		balance  uint64
		retained bool
	}{
		{name: "without journal", balance: 1000000},
		{name: "unaffordable journal", journal: true, balance: 21100},
		{name: "affordable journal", journal: true, balance: 1000000, retained: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, _ := crypto.GenerateKey()
			from := crypto.PubkeyToAddress(key.PublicKey)
			tx := transaction(0, 21000, key)
			statedb, err := state.New(types.EmptyRootHash, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
			if err != nil {
				t.Fatal(err)
			}
			statedb.SetBalance(from, uint256.NewInt(tc.balance))
			statedb.SetState(types.L1BlockAddr, types.L1BaseFeeSlot, common.BigToHash(big.NewInt(1)))
			statedb.SetState(types.L1BlockAddr, types.ScalarSlot, common.BigToHash(big.NewInt(1000000)))
			chainConfig := *params.TestChainConfig
			chainConfig.Optimism = &params.OptimismConfig{}
			config := testTxPoolConfig
			if tc.journal {
				config.Journal = filepath.Join(t.TempDir(), "transactions.rlp")
				journal := newTxJournal(config.Journal)
				if err := journal.rotate(map[common.Address]types.Transactions{from: {tx}}); err != nil {
					t.Fatal(err)
				}
				if err := journal.close(); err != nil {
					t.Fatal(err)
				}
			}
			chain := newTestBlockChain(&chainConfig, 10000000, statedb, nil)
			pool := New(config, chain)
			if err := pool.Init(config.PriceLimit, chain.CurrentBlock(), makeAddressReserver()); err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			<-pool.initDoneCh
			pool.mu.Lock()
			defer pool.mu.Unlock()
			if have := pool.all.Get(tx.Hash()) != nil; have != tc.retained {
				t.Errorf("journal retained transaction = %v, want %v", have, tc.retained)
			}
			if pool.l1CostFn == nil {
				t.Fatal("L1 estimator not initialized before journal replay")
			}
			if list := pool.pending[from]; list != nil {
				want := new(big.Int).Add(tx.Cost(), pool.l1CostFn(tx.RollupCostData()))
				if list.totalcost.ToBig().Cmp(want) != 0 {
					t.Fatalf("recovered reservation = %v, want %v", list.totalcost, want)
				}
			}
		})
	}
}

func TestLegacyPoolL1CostFilterChanges(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	for _, pending := range []bool{false, true} {
		for _, tc := range []struct {
			name              string
			initialFee, fee   *big.Int
			disable, retained bool
		}{
			{name: "fee increase", initialFee: big.NewInt(100), fee: big.NewInt(2000000)},
			{name: "nil becomes fee", fee: big.NewInt(2000000)},
			{name: "L1 conversion overflow", initialFee: big.NewInt(100), fee: new(big.Int).Add(max, big.NewInt(1))},
			{name: "L1 sum overflow", initialFee: big.NewInt(100), fee: max},
			{name: "estimator disabled", initialFee: big.NewInt(2000000), disable: true, retained: true},
		} {
			name := "queue/" + tc.name
			if pending {
				name = "pending/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				defer func() {
					if err := recover(); err != nil {
						t.Errorf("fee change caused a panic: %v", err)
					}
				}()
				pool, key := setupPool()
				defer pool.Close()
				from := crypto.PubkeyToAddress(key.PublicKey)
				nonce := uint64(1)
				if pending {
					nonce = 0
				}
				tx := transaction(nonce, 50000, key)
				fee := tc.initialFee
				pool.mu.Lock()
				pool.currentState.SetBalance(from, uint256.NewInt(10000000))
				pool.l1CostFn = func(types.RollupCostData) *big.Int { return fee }
				pool.mu.Unlock()
				if err := pool.addRemoteSync(tx); err != nil {
					t.Fatal(err)
				}
				pool.mu.Lock()
				defer pool.mu.Unlock()
				list := pool.queue[from]
				if pending {
					list = pool.pending[from]
				}
				if list == nil {
					t.Fatal("initial admission did not populate the expected list")
				}
				reserved := new(uint256.Int).Set(list.totalcost)
				pool.currentState.SetBalance(from, uint256.NewInt(100000))
				fee = tc.fee
				if tc.disable {
					pool.l1CostFn = nil
				}
				if pending {
					pool.demoteUnexecutables(nil)
				} else {
					pool.promoteExecutables([]common.Address{from})
				}
				if have := pool.all.Get(tx.Hash()) != nil; have != tc.retained {
					t.Errorf("retained transaction = %v, want %v", have, tc.retained)
				}
				if tc.retained {
					if !list.totalcost.Eq(reserved) {
						t.Error("filter changed the insertion-time reservation")
					}
				} else if !list.totalcost.IsZero() {
					t.Errorf("removed transaction left cost %v", list.totalcost)
				}
			})
		}
	}
}
