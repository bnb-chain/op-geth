package types

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"testing"
)

func TestBundleSimulationIdentity(t *testing.T) {
	txs := Transactions{NewTx(&LegacyTx{Nonce: 0, Gas: 21000}), NewTx(&LegacyTx{Nonce: 1, Gas: 21000})}
	a := &Bundle{Txs: txs, RevertingTxHashes: []common.Hash{txs[0].Hash(), txs[1].Hash()}}
	b := &Bundle{Txs: txs, RevertingTxHashes: []common.Hash{txs[1].Hash(), txs[0].Hash()}, MaxTimestamp: 123, MaxBlockNumber: 10, Price: big.NewInt(100)}
	if a.SimulationHash() != b.SimulationHash() {
		t.Fatal("non-execution metadata changed simulation identity")
	}
	b.Txs = Transactions{txs[1], txs[0]}
	if a.SimulationHash() == b.SimulationHash() {
		t.Fatal("transaction order missing from identity")
	}
	b.Txs = txs
	b.RevertingTxHashes = nil
	if a.SimulationHash() == b.SimulationHash() {
		t.Fatal("revert policy missing from identity")
	}
}
func TestBundleValidation(t *testing.T) {
	tx := NewTx(&LegacyTx{Gas: 21000})
	for _, tt := range []struct {
		name  string
		b     *Bundle
		valid bool
	}{
		{"valid", &Bundle{Txs: Transactions{tx}, RevertingTxHashes: []common.Hash{tx.Hash()}}, true},
		{"empty", &Bundle{}, false},
		{"nil transaction", &Bundle{Txs: Transactions{nil}}, false},
		{"duplicate revert", &Bundle{Txs: Transactions{tx}, RevertingTxHashes: []common.Hash{tx.Hash(), tx.Hash()}}, false},
		{"unknown revert", &Bundle{Txs: Transactions{tx}, RevertingTxHashes: []common.Hash{{1}}}, false},
		{"oversized", &Bundle{Txs: Transactions{NewTx(&LegacyTx{Gas: 1000000, Data: make([]byte, 512*1024)})}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.b.ValidateBasic() == nil; got != tt.valid {
				t.Fatalf("valid=%v, want %v", got, tt.valid)
			}
		})
	}
}
