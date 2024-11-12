package state

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestPevmJournalOfState(t *testing.T) {
	var snapid int
	var (
		Address1 = common.Address{0x01}
		Address2 = common.Address{0x02}
		Address3 = common.Address{0x03}
		Address4 = common.Address{0x04}
	)
	prepare := Tx{
		{"SetNonce", Address1, 1},
		{"AddBalance", Address2, big.NewInt(100)},
		{"SetState", Address3, "key", "value"},
		{"SetCode", Address4, []byte{0x04}},
	}
	// set states that will be reverted
	tx := Tx{
		{"Snapshot", &snapid},
		{"SetNonce", Address1, 2},
		{"AddBalance", Address2, big.NewInt(100)},
		{"SetState", Address3, "key", "value3"},
		{"SetCode", Address4, []byte{0x040}},
	}
	revertTx := Tx{
		{"Revert", &snapid},
	}
	beforeRevert := Checks{
		{"nonce", Address1, 2},
		{"balance", Address2, big.NewInt(200)},
		{"state", Address3, "key", "value3"},
		{"code", Address4, []byte{0x040}},
	}
	afterRevert := Checks{
		{"nonce", Address1, 1},
		{"balance", Address2, big.NewInt(100)},
		{"state", Address3, "key", "value"},
		{"code", Address4, []byte{0x04}},
	}

	statedb := newStateDB()
	prepare.Call(statedb)
	tx.Call(statedb)
	if err := beforeRevert.Verify(statedb); err != nil {
		t.Fatalf("[maindb]before revert: %v", err)
	}
	revertTx.Call(statedb)
	if err := afterRevert.Verify(statedb); err != nil {
		t.Fatalf("[maindb]after revert: %v", err)
	}

	uncommitted := NewUncommittedDB(newStateDB())
	prepare.Call(uncommitted)
	tx.Call(uncommitted)
	if err := beforeRevert.Verify(uncommitted); err != nil {
		t.Fatalf("[uncommitted]before revert: %v", err)
	}
	revertTx.Call(uncommitted)
	if err := afterRevert.Verify(uncommitted); err != nil {
		t.Fatalf("[uncommitted]after revert: %v", err)
	}
}

func TestPevmJournal(t *testing.T) {
	var snapid int
	var tx1 = common.Hash{0x012}
	var txIndex = 20
	var (
		Address1 = common.Address{0x01}
		Address2 = common.Address{0x02}
		Address3 = common.Address{0x03}
		Address4 = common.Address{0x04}
		Address5 = common.Address{0x05}
		Address6 = common.Address{0x06}
	)
	tx := Tx{
		{"Snapshot", &snapid},
		{"SetNonce", Address1, 1},
		{"AddBalance", Address2, big.NewInt(100)},
		{"SetState", Address3, "key", "value"},
		{"SetCode", Address4, []byte{0x04}},
		{"Create", Address5},
		{"AddPreimage", common.Hash{0x3}, []byte{0x05}},
		{"SetTransientStorage", Address6, "hash", "tx"},
		{"AddRefund", 102},
		{"AddLog", &types.Log{TxHash: tx1, Data: []byte("hello"), TxIndex: uint(txIndex)}},
		{"AddLog", &types.Log{TxHash: tx1, Data: []byte("world"), TxIndex: uint(txIndex)}},
		{"AddAddress", Address1},
		{"AddSlots", Address2, common.Hash{0x22}},
	}
	revertTx := Tx{
		{"Revert", &snapid},
	}
	beforeRevert := Checks{
		{"nonce", Address1, 1},
		{"balance", Address2, big.NewInt(100)},
		{"state", Address3, "key", "value"},
		{"code", Address4, []byte{0x04}},
		{"exists", Address5, true},
		{"preimage", common.Hash{0x3}, []byte{0x05}},
		{"tstorage", Address6, "hash", "tx"},
		{"refund", 102},
		{"log", tx1, 0, []byte("hello"), txIndex, 0},
		{"log", tx1, 1, []byte("world"), txIndex, 1},
		{"loglen", 2},
		{"address", Address1, true},
		{"address", Address2, true},
		{"slot", Address2, common.Hash{0x22}, true},
	}
	afterRevert := Checks{
		{"exists", Address1, false},
		{"exists", Address2, false},
		{"exists", Address3, false},
		{"exists", Address4, false},
		{"exists", Address5, false},
		{"preimageExists", common.Hash{0x3}, false},
		{"tstorage", Address6, "hash", ""},
		{"refund", 0},
		{"loglen", 0},
		{"address", Address1, false},
		{"address", Address2, false},
		{"slot", Address2, common.Hash{0x22}, false},
	}

	statedb := newStateDB()
	statedb.SetTxContext(tx1, txIndex)
	tx.Call(statedb)
	if err := beforeRevert.Verify(statedb); err != nil {
		t.Fatalf("[maindb]before revert: %v", err)
	}
	revertTx.Call(statedb)
	if err := afterRevert.Verify(statedb); err != nil {
		t.Fatalf("[maindb]after revert: %v", err)
	}

	uncommitted := NewUncommittedDB(newStateDB())
	uncommitted.SetTxContext(tx1, txIndex)
	tx.Call(uncommitted)
	if err := beforeRevert.Verify(uncommitted); err != nil {
		t.Fatalf("[uncommitted]before revert: %v", err)
	}
	revertTx.Call(uncommitted)
	if err := afterRevert.Verify(uncommitted); err != nil {
		t.Fatalf("[uncommitted]after revert: %v", err)
	}
}
