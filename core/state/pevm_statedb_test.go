package state

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

var Address1 = common.HexToAddress("0x1")
var Address2 = common.HexToAddress("0x2")

func TestInvalidGasUsed(t *testing.T) {
	txs := Txs{
		{ // prepare for the account
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)}, // make the object not empty
			{"SetState", Address1, "key1", "value1"},
			{"SetCode", Address1, []byte("hello")},
		},
		{
			{"SetNonce", Address1, 1}, // make the object not empty
			{"SelfDestruct", Address1},
		},
		{
			{"GetCodeHash", Address1, common.Hash{0x1}},
			{"GetNonce", Address1, 0},
			{"Create", Address1},      // re-create the object
			{"SetNonce", Address1, 1}, // make the object not empty
		},
	}
	checks := []Checks{
		// after execute tx1, before finalize
		{
			{"state", Address1, "key1", "value1"},
			{"code", Address1, []byte("hello")},
		},
		//after finalize tx1,
		{
			{"state", Address1, "key1", ""},
			{"code", Address1, []byte{}},
		},
		// after execute tx1, before && after finalize
		{
			{"state", Address1, "key1", ""},
			{"code", Address1, []byte{}},
		},
	}

	verifyDBs := func(c Checks, maindb *StateDB, uncommited *UncommittedDB) error {
		if c.Verify(maindb) != nil {
			return fmt.Errorf("maindb: %s", c.Verify(maindb).Error())
		}
		if c.Verify(uncommited) != nil {
			return fmt.Errorf("uncommited: %s", c.Verify(uncommited).Error())
		}
		return nil
	}

	maindb := newStateDB()
	shadow := newStateDB()
	// firtst prepare for the account, to ensure the selfDestruct happens
	txs[0].Call(maindb)
	txs[0].Call(shadow)
	maindb.Finalise(true)
	shadow.Finalise(true)

	uncommitted := newUncommittedDB(maindb)
	txs[1].Call(uncommitted)
	txs[1].Call(shadow)
	if err := verifyDBs(checks[0], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now finalize the data to maindb
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// now check the state after finalize
	if err := verifyDBs(checks[1], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now execute another tx
	uncommitted = newUncommittedDB(maindb)
	txs[2].Call(uncommitted)
	txs[2].Call(shadow)
	if err := verifyDBs(checks[2], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// check the state again, to ensure the Finalize works
	if err := verifyDBs(checks[2], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestStateAfterDestructWithBalance(t *testing.T) {
	// tx0:
	//		1. create an account -> key1: nil
	//		2. set state		 -> key1: value1
	//		3. self destruct 	 -> key1: nil
	// tx1:
	//		1. create an account -> key1: nil
	//		2. set state		 -> key1: value2
	//		3. self destruct 	 -> key1: nil
	txs := Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)}, // balance should not be carried over
			{"SetNonce", Address1, 1},                 // make the object not empty
			{"SetState", Address1, "key1", "value1"},
			{"SelfDestruct", Address1},
		},
		{
			{"Create", Address1},
			{"SetNonce", Address1, 1}, // make the object not empty
			{"SetState", Address1, "key1", "value2"},
		},
	}
	checks := []Checks{
		// after execute tx0, before finalize
		{
			{"state", Address1, "key1", "value1"},
			{"balance", Address1, big.NewInt(0)},
		},
		//after finalize tx0,
		{
			{"state", Address1, "key1", ""},
			{"balance", Address1, big.NewInt(0)},
		},
		// after execute tx1, before && after finalize
		{
			{"state", Address1, "key1", "value2"},
			{"balance", Address1, big.NewInt(0)},
		},
	}

	verifyDBs := func(c Checks, maindb *StateDB, uncommited *UncommittedDB) error {
		if c.Verify(maindb) != nil {
			return fmt.Errorf("maindb: %s", c.Verify(maindb).Error())
		}
		if c.Verify(uncommited) != nil {
			return fmt.Errorf("uncommited: %s", c.Verify(uncommited).Error())
		}
		return nil
	}

	maindb := newStateDB()
	shadow := newStateDB()
	uncommitted := newUncommittedDB(maindb)
	txs[0].Call(uncommitted)
	txs[0].Call(shadow)
	if err := verifyDBs(checks[0], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now finalize the data to maindb
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// now check the state after finalize
	if err := verifyDBs(checks[1], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now execute another tx
	uncommitted = newUncommittedDB(maindb)
	txs[1].Call(uncommitted)
	txs[1].Call(shadow)
	if err := verifyDBs(checks[2], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// check the state again, to ensure the Finalize works
	if err := verifyDBs(checks[2], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestStateAfterDestruct(t *testing.T) {
	// tx0:
	//		1. create an account -> key1: nil
	//		2. set state		 -> key1: value1
	//		3. self destruct 	 -> key1: nil
	// tx1:
	//		1. create an account -> key1: nil
	//		2. set state		 -> key1: value2
	//		3. self destruct 	 -> key1: nil
	txs := Txs{
		{
			{"Create", Address1},
			{"SetNonce", Address1, 1}, // make the object not empty
			{"SetState", Address1, "key1", "value1"},
		},
		{
			{"SelfDestruct", Address1},
		},
		{
			{"Create", Address1},
			{"SetNonce", Address1, 1}, // make the object not empty
			{"SetState", Address1, "key1", "value2"},
		},
		{
			{"SelfDestruct", Address1},
		},
	}
	checks := []Check{
		{"state", Address1, "key1", "value1"},
		{"state", Address1, "key1", ""},
		{"state", Address1, "key1", "value2"},
		{"state", Address1, "key1", ""},
	}

	verifyDBs := func(c Check, maindb *StateDB, uncommited *UncommittedDB) error {
		if c.Verify(maindb) != nil {
			return fmt.Errorf("maindb: %s", c.Verify(maindb).Error())
		}
		if c.Verify(uncommited) != nil {
			return fmt.Errorf("uncommited: %s", c.Verify(uncommited).Error())
		}
		return nil
	}

	maindb := newStateDB()
	shadow := newStateDB()
	uncommitted := newUncommittedDB(maindb)
	txs[0].Call(uncommitted)
	txs[0].Call(shadow)
	// key1: value1
	if err := verifyDBs(checks[0], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	txs[1].Call(uncommitted)
	txs[1].Call(shadow)
	beforeMerge := checks[0]
	if err := verifyDBs(beforeMerge, shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now finalize the data to maindb
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// check the state again
	if err := verifyDBs(checks[1], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// now recreate the account
	uncommitted = newUncommittedDB(maindb)
	// check the state again, to make sure the newly create uncommited have the same state of the shadow db
	if err := verifyDBs(checks[1], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	txs[2].Call(uncommitted)
	txs[2].Call(shadow)
	beforeMerge = checks[2]
	if err := verifyDBs(beforeMerge, shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// check the state again, to ensure the Finalize works
	if err := verifyDBs(checks[2], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	uncommitted = newUncommittedDB(maindb)
	txs[3].Call(uncommitted)
	txs[3].Call(shadow)
	// check the state again, to ensure the Finalize works
	beforeMerge = checks[2]
	if err := verifyDBs(beforeMerge, shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	uncommitted.Merge(true)
	maindb.Finalise(true)
	shadow.Finalise(true)
	// check the state again, to ensure the Finalize works
	if err := verifyDBs(checks[3], shadow, uncommitted); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

// TestUncommitedDBCreateAccount tests the creation of an account in an uncommited DB.
func TestPevmUncommitedDBCreateAccount(t *testing.T) {
	// case 1. create an account without previous state
	txs := Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SetNonce", Address1, 2},
		},
	}
	check := CheckState{
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"balance", Address1, big.NewInt(100)},
				{"nonce", Address1, 2},
			},
			Maindb: []Check{
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 0},
			},
		},
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(100)},
			{"nonce", Address1, 2},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

	// case 2. create an account with previous state
	txs = Txs{
		// previous state
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(200)},
			{"SetNonce", Address1, 2},
		},
		// create a new account, should have a balance of 100, and nonce = 0
		{
			{"Create", Address1},
		},
	}
	check = CheckState{
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(200)},
			{"nonce", Address1, 0},
		},
	}
	if er := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); er != nil {
		t.Fatalf("ut failed, err=%s", er.Error())
	}
}

func TestPevmAddBalance(t *testing.T) {
	// case 1. add balance to an account without previous state
	txs := Txs{
		{
			{"AddBalance", Address1, big.NewInt(100)},
		},
	}
	check := CheckState{
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(100)},
			{"nonce", Address1, 0},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

func TestPevmSubBalance(t *testing.T) {
	txs := Txs{
		{
			{"AddBalance", Address1, big.NewInt(120)},
			{"SubBalance", Address1, big.NewInt(100)},
			{"SetNonce", Address1, 2},
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 0},
			},
			Maindb: []Check{
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 0},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"balance", Address1, big.NewInt(20)},
				{"nonce", Address1, 2},
			},
			Maindb: []Check{
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 0},
			},
		},
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(20)},
			{"nonce", Address1, 2},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

func TestPevmSetCode(t *testing.T) {
	// case 1. set code to an account without previous state
	txs := Txs{
		{
			{"SetCode", Address1, []byte("hello")},
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"code", Address1, []byte("")},
			},
			Maindb: []Check{
				{"code", Address1, []byte("")},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"code", Address1, []byte("hello")},
			},
			Maindb: []Check{
				{"code", Address1, []byte("")},
			},
		},
		AfterMerge: []Check{
			{"code", Address1, []byte("hello")},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 2.
	txs = Txs{
		{
			{"Create", Address1},
			{"SetCode", Address1, []byte("hello")},
			{"AddBalance", Address1, big.NewInt(100)},
		},
	}
	check = CheckState{
		AfterMerge: []Check{
			{"code", Address1, []byte("hello")},
			{"balance", Address1, big.NewInt(100)},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmSetState(t *testing.T) {
	// 1. set state to an account without previous state, check getState, getCommittedState
	txs := Txs{
		{
			{"SetState", Address1, "key1", "value1"},
			{"SetState", Address1, "key2", "value2"},
			{"SetNonce", Address1, 1}, // to avoid the deletion when finalise
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"state", Address1, "key1", ""},
				{"state", Address1, "key2", ""},
				{"obj", Address1, "nil"},
			},
			Maindb: []Check{
				{"state", Address1, "key1", ""},
				{"state", Address1, "key2", ""},
				{"obj", Address1, "nil"},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"state", Address1, "key1", "value1"},
				{"state", Address1, "key2", "value2"},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"state", Address1, "key1", ""},
				{"state", Address1, "key2", ""},
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"state", Address1, "key1", "value1"},
			{"state", Address1, "key2", "value2"},
			{"cstate", Address1, "key1", "value1"},
			{"cstate", Address1, "key2", "value2"},
			{"obj", Address1, "exists"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// 1. set state to an account without previous state, check getState, getCommittedState
	prepare := Txs{
		{
			{"Create", Address1},
			{"SetState", Address1, "key0", "value0"},
			{"SetState", Address1, "key1", "valuea"},
			{"AddBalance", Address1, big.NewInt(100)}, // to avoid the deletion when finalise
		},
	}
	statedb := newStateDB()
	prepare.Call(statedb)
	sroot := statedb.IntermediateRoot(true)

	maindb := newStateDB()
	prepare.Call(maindb)
	proot := maindb.IntermediateRoot(true)
	if sroot.Cmp(proot) != 0 {
		t.Fatalf("ut failed, err=%s", "roots mismatch")
	}
	uncommitedDB := newUncommittedDB(maindb)

	txs = Txs{
		{
			{"SetState", Address1, "key1", "value1"},
			{"SetState", Address1, "key2", "value2"},
		},
	}
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"state", Address1, "key1", "valuea"},
				{"state", Address1, "key2", ""},
				{"cstate", Address1, "key1", "valuea"},
				{"cstate", Address1, "key0", "value0"},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"state", Address1, "key1", "valuea"},
				{"state", Address1, "key2", ""},
				{"cstate", Address1, "key1", "valuea"},
				{"cstate", Address1, "key0", "value0"},
				{"obj", Address1, "exists"},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"state", Address1, "key1", "value1"},
				{"state", Address1, "key2", "value2"},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"obj", Address1, "exists"},
				{"state", Address1, "key1", "valuea"},
				{"state", Address1, "key2", ""},
			},
		},
		AfterMerge: []Check{
			{"state", Address1, "key0", "value0"},
			{"state", Address1, "key1", "value1"},
			{"state", Address1, "key2", "value2"},
			{"cstate", Address1, "key0", "value0"},
			{"cstate", Address1, "key1", "value1"},
			{"cstate", Address1, "key2", "value2"},
			{"obj", Address1, "exists"},
			{"balance", Address1, big.NewInt(100)},
		},
	}
	if err := runCase(txs, statedb, uncommitedDB, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmSelfDestructStateDB(t *testing.T) {
	//1. create address1, set code, set balance, set nonce
	//2. self destruct address1
	//3. check the state of address1 before finalize
	//4. finalize the stateDB
	//5. check the state of address1 after finalize
	statedb := newStateDB()
	uncommitedState := newUncommittedDB(newStateDB())
	prepare := Txs{
		{
			{"Create", Address1},
			{"SetNonce", Address1, 12},
			{"SetCode", Address1, []byte("hello world")},
			{"AddBalance", Address1, big.NewInt(100)},
		},
	}
	prepare.Call(statedb)
	prepare.Call(uncommitedState)
	checks := Checks{
		{"code", Address1, []byte("hello world")},
		{"balance", Address1, big.NewInt(100)},
		{"nonce", Address1, 12},
	}
	if err := checks.Verify(statedb); err != nil {
		t.Fatalf("unexpected prepared state, err=%s", err.Error())
	}
	if err := checks.Verify(uncommitedState); err != nil {
		t.Fatalf("unexpected prepared state, err=%s", err.Error())
	}
	destruct := Txs{
		{
			{"SelfDestruct", Address1},
		},
	}
	destruct.Call(statedb)
	destruct.Call(uncommitedState)
	// check the state of address1 before finalise: they should keep the same except the balance
	checks = Checks{
		{"code", Address1, []byte("hello world")},
		{"balance", Address1, big.NewInt(0)},
		{"nonce", Address1, 12},
	}
	if err := checks.Verify(statedb); err != nil {
		t.Fatalf("[statedb] unexpected selfdestruct state before finalized, err=%s", err.Error())
	}
	if err := checks.Verify(uncommitedState); err != nil {
		t.Fatalf("[uncommitted] unexpected selfdestruct state before finalized, err=%s", err.Error())
	}
	statedb.Finalise(true)
	uncommitedState.Merge(true)
	uncommitedState.maindb.Finalise(true)
	checks = Checks{
		{"code", Address1, []byte(nil)},
		{"balance", Address1, big.NewInt(0)},
		{"nonce", Address1, 0},
	}
	// check the state of address1 after finalise: they should be all nil
	if err := checks.Verify(statedb); err != nil {
		t.Fatalf("[statedb] unexpected selfdestruct state after finalized, err=%s", err.Error())
	}
	if err := checks.Verify(uncommitedState.maindb); err != nil {
		t.Fatalf("[uncommitted] unexpected selfdestruct state after finalized, err=%s", err.Error())
	}
	statedb.IntermediateRoot(true)
	uncommitedState.maindb.IntermediateRoot(true)
	// check the state of address1 after finalise: they should be all nil
	checks = Checks{
		{"code", Address1, []byte(nil)},
		{"balance", Address1, big.NewInt(0)},
		{"nonce", Address1, 0},
		{"obj", Address1, "nil"},
	}
	if err := checks.Verify(statedb); err != nil {
		t.Fatalf("[statedb] unexpected selfdestruct state after committed, err=%s", err.Error())
	}
	if err := checks.Verify(uncommitedState.maindb); err != nil {
		t.Fatalf("[unstate.maindb] unexpected selfdestruct state after committed, err=%s", err.Error())
	}
}

func TestPevmSelfDestruct(t *testing.T) {
	// case 1. self destruct an account without previous state
	txs := Txs{
		{
			{"SelfDestruct", Address1},
		},
	}
	check := CheckState{
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"obj", Address1, "nil"},
			},
			Maindb: []Check{
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 2. selfdestruct an account who exists in uncommited db but not maindb
	txs = Txs{
		{
			{"Create", Address1},
			{"SetNonce", Address1, 12},
			{"SetCode", Address1, []byte("hello world")},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SelfDestruct", Address1},
		},
	}
	check = CheckState{
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"code", Address1, []byte("hello world")},
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 12},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"codeHash", Address1, common.Hash{}}, // an empty hash will be returned if the account does not exist
				{"code", Address1, []byte(nil)},
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 0},
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"codeHash", Address1, common.Hash{}},
			{"code", Address1, []byte(nil)},
			{"balance", Address1, big.NewInt(0)},
			{"nonce", Address1, 0},
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 3. destruct an account who exist in maindb but not in uncommited db
	prepare := Txs{
		{
			{"Create", Address1},
			{"SetNonce", Address1, 12},
			{"SetCode", Address1, []byte("hello world")},
			{"AddBalance", Address1, big.NewInt(100)},
		},
	}
	statedb := newStateDB()
	maindb := newStateDB()
	prepare.Call(statedb)
	prepare.Call(maindb)
	UncommittedDB := newUncommittedDB(maindb)
	txs = Txs{
		{
			{"SelfDestruct", Address1},
		},
	}
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"code", Address1, []byte("hello world")},
				{"balance", Address1, big.NewInt(100)},
				{"nonce", Address1, 12},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"code", Address1, []byte("hello world")},
				{"balance", Address1, big.NewInt(100)},
				{"nonce", Address1, 12},
				{"obj", Address1, "exists"},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"code", Address1, []byte("hello world")},
				{"balance", Address1, big.NewInt(0)},
				{"nonce", Address1, 12},
				{"obj", Address1, "exists"},
			},
			Maindb: []Check{
				{"code", Address1, []byte("hello world")},
				{"balance", Address1, big.NewInt(100)},
				{"nonce", Address1, 12},
				{"obj", Address1, "exists"},
			},
		},
		AfterMerge: []Check{
			{"codeHash", Address1, common.Hash{}},
			{"code", Address1, []byte(nil)},
			{"balance", Address1, big.NewInt(0)},
			{"nonce", Address1, 0},
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, statedb, UncommittedDB, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 3. create an account after self destruct
	txs = Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SelfDestruct", Address1},
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(50)},
		},
	}
	check = CheckState{
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(50)},
			{"nonce", Address1, 0},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmSelfDestruct6780AndRevert(t *testing.T) {
	shadow := newStateDB()
	uncommitted := newUncommittedDB(newStateDB())
	//prepare: create an account, set state, set balance
	// A1{key1: val1, balance: 100, accesslist: 0x33}
	prepare := Tx{
		{"Create", Address1},
		{"SetState", Address1, "key1", "val1"},
		{"AddBalance", Address1, big.NewInt(100)},
		{"AddSlots", Address1, common.Hash{0x33}},
	}
	prepareCheck := Checks{
		{"balance", Address1, big.NewInt(100)},
		{"state", Address1, "key1", "val1"},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	check := func(verify Checks, shadow, uncommitted vm.StateDB) {
		if err := verify.Verify(shadow); err != nil {
			t.Fatalf("maindb verify failed, err=%s", err.Error())
		}
		if err := verify.Verify(uncommitted); err != nil {
			t.Fatalf("uncommitted verify failed, err=%s", err.Error())
		}
	}
	prepare.Call(shadow)
	prepare.Call(uncommitted)
	check(prepareCheck, shadow, uncommitted)

	// do the following operations, and then selfdestruct, and then revert
	// 1. add balance 200,
	// 2. set state key1: val2
	// 3. add slot 0x34
	// 4. selfdestruct
	// 5. revert
	case1 := Tx{
		{"AddBalance", Address1, big.NewInt(200)},
		{"SetState", Address1, "key1", "val2"},
		{"SetState", Address1, "key2", "val0"},
		{"AddSlots", Address1, common.Hash{0x34}},
	}
	case1SelfDestruct := Tx{
		{"SelfDestruct6780", Address1},
	}
	beforeSelfDestruct := Checks{
		{"balance", Address1, big.NewInt(300)},
		{"state", Address1, "key1", "val2"},
		{"state", Address1, "key2", "val0"},
		{"slot", Address1, common.Hash{0x34}, true},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	beforeRevert := Checks{
		{"balance", Address1, big.NewInt(0)},
		{"state", Address1, "key1", "val2"},
		{"state", Address1, "key2", "val0"},
		{"slot", Address1, common.Hash{0x34}, true},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	afterRevert := Checks{
		{"balance", Address1, big.NewInt(100)},
		{"state", Address1, "key1", "val1"},
		{"state", Address1, "key2", ""},
		{"slot", Address1, common.Hash{0x34}, false},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	// run the case1 on shadow
	snapshot := shadow.Snapshot()
	case1.Call(shadow)
	if err := beforeSelfDestruct.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}
	case1SelfDestruct.Call(shadow)
	if err := beforeRevert.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}
	shadow.RevertToSnapshot(snapshot)
	if err := afterRevert.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}

	// now on uncommitted
	snapshot = uncommitted.Snapshot()
	case1.Call(uncommitted)
	if err := beforeSelfDestruct.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}
	case1SelfDestruct.Call(uncommitted)
	if err := beforeRevert.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}
	uncommitted.RevertToSnapshot(snapshot)
	if err := afterRevert.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}

	// now compare the mercle root of shadow and uncommitted
	shadow.Finalise(true)
	if err := uncommitted.Merge(true); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
	uncommitted.Finalise(true)
	if shadow.IntermediateRoot(true) != uncommitted.maindb.IntermediateRoot(true) {
		t.Fatalf("ut failed, err: the mercle root of shadow and uncommitted are different")
	}
}

func TestPevmSelfDestructAndRevert(t *testing.T) {
	shadow := newStateDB()
	uncommitted := newUncommittedDB(newStateDB())
	//prepare: create an account, set state, set balance
	// A1{key1: val1, balance: 100, accesslist: 0x33}
	prepare := Tx{
		{"Create", Address1},
		{"SetState", Address1, "key1", "val1"},
		{"AddBalance", Address1, big.NewInt(100)},
		{"AddSlots", Address1, common.Hash{0x33}},
	}
	prepareCheck := Checks{
		{"balance", Address1, big.NewInt(100)},
		{"state", Address1, "key1", "val1"},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	check := func(verify Checks, shadow, uncommitted vm.StateDB) {
		if err := verify.Verify(shadow); err != nil {
			t.Fatalf("maindb verify failed, err=%s", err.Error())
		}
		if err := verify.Verify(uncommitted); err != nil {
			t.Fatalf("uncommitted verify failed, err=%s", err.Error())
		}
	}
	prepare.Call(shadow)
	prepare.Call(uncommitted)
	shadow.Finalise(true)
	uncommitted.Finalise(true)
	check(prepareCheck, shadow, uncommitted)

	// do the following operations, and then selfdestruct, and then revert
	// 1. add balance 200,
	// 2. set state key1: val2
	// 3. add slot 0x34
	// 4. selfdestruct
	// 5. revert
	case1 := Tx{
		{"AddBalance", Address1, big.NewInt(200)},
		{"SetState", Address1, "key1", "val2"},
		{"SetState", Address1, "key2", "val0"},
		{"AddSlots", Address1, common.Hash{0x34}},
	}
	case1SelfDestruct := Tx{
		{"SelfDestruct", Address1},
	}
	beforeSelfDestruct := Checks{
		{"balance", Address1, big.NewInt(300)},
		{"state", Address1, "key1", "val2"},
		{"state", Address1, "key2", "val0"},
		{"slot", Address1, common.Hash{0x34}, true},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	beforeRevert := Checks{
		{"balance", Address1, big.NewInt(0)},
		{"state", Address1, "key1", "val2"},
		{"state", Address1, "key2", "val0"},
		{"slot", Address1, common.Hash{0x34}, true},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	afterRevert := Checks{
		{"balance", Address1, big.NewInt(100)},
		{"state", Address1, "key1", "val1"},
		{"state", Address1, "key2", ""},
		{"slot", Address1, common.Hash{0x34}, false},
		{"slot", Address1, common.Hash{0x33}, true},
	}
	// run the case1 on shadow
	snapshot := shadow.Snapshot()
	case1.Call(shadow)
	if err := beforeSelfDestruct.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}
	case1SelfDestruct.Call(shadow)
	if err := beforeRevert.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}
	shadow.RevertToSnapshot(snapshot)
	if err := afterRevert.Verify(shadow); err != nil {
		t.Fatalf("maindb ut failed, err:%s", err.Error())
	}

	// now on uncommitted
	snapshot = uncommitted.Snapshot()
	case1.Call(uncommitted)
	if err := beforeSelfDestruct.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}
	case1SelfDestruct.Call(uncommitted)
	if err := beforeRevert.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}
	uncommitted.RevertToSnapshot(snapshot)
	if err := afterRevert.Verify(uncommitted); err != nil {
		t.Fatalf("uncommitted ut failed, err:%s", err.Error())
	}

	// now compare the mercle root of shadow and uncommitted
	shadow.Finalise(true)
	if err := uncommitted.Merge(true); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
	uncommitted.Finalise(true)
	if shadow.IntermediateRoot(true) != uncommitted.maindb.IntermediateRoot(true) {
		t.Fatalf("ut failed, err: the mercle root of shadow and uncommitted are different")
	}
}

func TestPevmSelfDestruct6780(t *testing.T) {
	// case 1. no previous state
	txs := Txs{
		{
			{"SelfDestruct6780", Address1},
		},
	}
	check := CheckState{
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"obj", Address1, "nil"},
			},
			Maindb: []Check{
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 2. self destruct an account with previous state
	txs = Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SelfDestruct6780", Address1},
		},
	}
	check = CheckState{
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(0)},
			{"nonce", Address1, 0},
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 3. create an account after self destruct
	txs = Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SetNonce", Address1, 10},
			{"SelfDestruct6780", Address1},
			{"Create", Address1},
		},
	}
	check = CheckState{
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(0)},
			{"nonce", Address1, 0},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
	// case 4. self destruct 6780 should not work if the account is not empty
	maindb := newStateDB()
	statedb := newStateDB()
	prepare := Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
			{"SetNonce", Address1, 2},
		},
	}
	prepare.Call(maindb)
	prepare.Call(statedb)
	maindb.IntermediateRoot(true)
	statedb.IntermediateRoot(true)
	txs = Txs{
		{
			{"AddBalance", Address1, big.NewInt(100)},
			{"SelfDestruct6780", Address1},
		},
	}
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"balance", Address1, big.NewInt(200)},
				{"nonce", Address1, 2},
			},
			Maindb: []Check{
				{"balance", Address1, big.NewInt(200)},
				{"nonce", Address1, 2},
			},
		},
		AfterMerge: []Check{
			{"balance", Address1, big.NewInt(200)},
			{"nonce", Address1, 2},
		},
	}
	if err := runCase(txs, statedb, newUncommittedDB(maindb), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

func TestPevmExistsAndEmpty(t *testing.T) {
	// case 1. exists an account without previous state
	txs := Txs{
		{
			{"Create", Address1},
			{"AddBalance", Address1, big.NewInt(100)},
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
			},
			Maindb: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"exists", Address1, true},
				{"empty", Address1, false},
			},
			Maindb: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"empty", Address1, false},
			{"exists", Address1, true},
			{"balance", Address1, big.NewInt(100)},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

	txs = Txs{
		{
			{"Create", Address1},
		},
	}
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
			},
			Maindb: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"exists", Address1, true}, //the account is created, but empty
				{"empty", Address1, true},
			},
			Maindb: []Check{
				{"exists", Address1, false},
				{"empty", Address1, true},
				{"obj", Address1, "nil"},
			},
		},
		AfterMerge: []Check{
			{"empty", Address1, true}, //empty accounts will be deleted after finalise
			{"exists", Address1, false},
			{"obj", Address1, "nil"},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmRefund(t *testing.T) {
	// case 1. add refund from 0
	txs := Txs{
		{
			{"AddRefund", 100},
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"refund", 0},
			},
			Maindb: []Check{
				{"refund", 0},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"refund", 100},
			},
			Maindb: []Check{
				{"refund", 0},
			},
		},
		AfterMerge: []Check{
			{"refund", 0}, //refund will be cleared after finalise
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

	// case 2. add refund to a new account
	statedb := newStateDB()
	unstateMain := newStateDB()
	unstateMain.refund, statedb.refund = 100, 100
	txs = Txs{
		{
			{"AddRefund", 100},
			{"SubRefund", 20},
		},
	}
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"refund", 100},
			},
			Maindb: []Check{
				{"refund", 100},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"refund", 180},
			},
			Maindb: []Check{
				{"refund", 100},
			},
		},
		AfterMerge: []Check{
			{"refund", 0},
		},
	}
	if err := runCase(txs, statedb, newUncommittedDB(unstateMain), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmPreimages(t *testing.T) {
	txs := Txs{
		{
			{"AddPreimage", common.BytesToHash([]byte("hello")), []byte("world")},
			{"AddPreimage", common.BytesToHash([]byte("hello")), []byte("world2")}, // the second one should be ignored
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"preimage", common.BytesToHash([]byte("hello")), []byte("")},
			},
			Maindb: []Check{
				{"preimage", common.BytesToHash([]byte("hello")), []byte("")},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"preimage", common.BytesToHash([]byte("hello")), []byte("world")},
			},
			Maindb: []Check{
				{"preimage", common.BytesToHash([]byte("hello")), []byte("")},
			},
		},
		AfterMerge: []Check{
			{"preimage", common.BytesToHash([]byte("hello")), []byte("world")},
		},
	}
	if err := runCase(txs, newStateDB(), newUncommittedDB(newStateDB()), check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}
}

func TestPevmLogs(t *testing.T) {
	// diff logs of two stateDBs:
	//	1. check thash
	//  2. check index
	//  3. check txIndex
	// log.thash, log.index, log.txIndex = s.txHash, s.logSize, s.txIndex
	// so we need to test it by running a true transaction, in which we call the log function
	txs := Txs{
		{
			{"AddLog", &types.Log{Data: []byte("hello")}},
			{"AddLog", &types.Log{Data: []byte("world")}},
		},
	}
	thash := common.BytesToHash([]byte("00000000000000000tx"))
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"loglen", 0},
			},
			Maindb: []Check{
				{"loglen", 0},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"loglen", 2},
				{"log", thash, 0, []byte("hello"), 20, 0}, // i,  Data, TxIndex, Index
				{"log", thash, 1, []byte("world"), 20, 1},
			},
			Maindb: []Check{
				{"loglen", 0},
			},
		},
		AfterMerge: []Check{
			{"loglen", 2},
			{"log", thash, 0, []byte("hello"), 20, 0},
			{"log", thash, 1, []byte("world"), 20, 1},
		},
	}
	state := newStateDB()
	maindb := newStateDB()
	unstate := newUncommittedDB(maindb)
	//set TxContext
	unstate.txIndex, state.txIndex, maindb.txIndex = 20, 20, 19
	unstate.txHash, state.thash, maindb.thash = thash, thash, common.Hash{}

	if err := runCase(txs, state, unstate, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

	// test the maindb with txIndex=-1
	txs = Txs{
		{
			{"AddLog", &types.Log{Data: []byte("hello")}},
			{"AddLog", &types.Log{Data: []byte("world")}},
		},
	}
	thash = common.BytesToHash([]byte("00000000000000000tx2"))
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"loglen", 0},
			},
			Maindb: []Check{
				{"loglen", 0},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"loglen", 2},
				{"log", thash, 0, []byte("hello"), 0, 0}, // i,  Data, TxIndex, Index
				{"log", thash, 1, []byte("world"), 0, 1},
			},
			Maindb: []Check{
				{"loglen", 0},
			},
		},
		AfterMerge: []Check{
			{"loglen", 2},
			{"log", thash, 0, []byte("hello"), 0, 0},
			{"log", thash, 1, []byte("world"), 0, 1},
		},
	}
	state = newStateDB()
	state.SetTxContext(thash, 0)
	unstate = newUncommittedDB(newStateDB())
	unstate.SetTxContext(thash, 0)
	if err := runCase(txs, state, unstate, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

func TestPevmAccessList(t *testing.T) {
	// before Berlin hardfork:
	//	 accesslist will be available in the whole scope of block
	// after Berlin hardfork:
	//   accesslist will be available only in the scope of the transaction, except those predefined.

	// before Berlin hardfork:
	// case 1.
	//		prepare: add address 1, add slot 1
	//      tx: add address 2, add slot 2
	//		check: address 1, slot 1, address 2, slot 2 are all in the accesslist
	coinBase := common.BytesToAddress([]byte("0xcoinbase"))
	sender := common.BytesToAddress([]byte("0xsender"))
	key1 := common.Hash{0x33}
	key2 := common.Hash{0x34}
	prepare := Txs{
		{
			{"AddAddress", Address1},
			{"AddSlots", Address1, key1},
		},
	}
	tx := Txs{
		{
			{"AddAddress", Address2},
			{"AddSlots", Address2, key2},
		},
	}
	check := CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"address", Address1, true},
				{"slot", Address1, key1, true},
			},
			Maindb: []Check{
				{"address", Address1, true},
				{"slot", Address1, key1, true},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"address", Address1, true},
				{"address", Address2, true},
				{"slot", Address1, key1, true},
				{"slot", Address2, key2, true},
				{"address", sender, false},
				{"address", coinBase, false},
			},
			Maindb: []Check{
				{"address", Address1, true},
				{"address", Address2, false},
				{"slot", Address1, key1, true},
				{"slot", Address2, key2, false},
			},
		},
		AfterMerge: []Check{
			{"address", Address1, true},
			{"address", Address2, true},
			{"slot", Address1, key1, true},
			{"slot", Address2, key2, true},
			{"address", sender, false},
			{"address", coinBase, false},
		},
	}
	statedb := newStateDB()
	maindb := newStateDB()
	prepare.Call(statedb)
	prepare.Call(maindb)

	statedb.Prepare(params.Rules{IsBerlin: false}, sender, coinBase, nil, nil, nil)
	uncommited := newUncommittedDB(maindb)
	uncommited.Prepare(params.Rules{IsBerlin: false}, sender, coinBase, nil, nil, nil)
	if err := runCase(tx, statedb, uncommited, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

	// after Berlin hardfork:
	//	case 2:
	//		prepare: add address 1, add slot 1
	//		tx: add address 2, add slot 2
	//		before merge:
	//			address 2, slot 2 are in the accesslist; but not address 1, slot 1
	//			predefined address n, slot n are in the accesslist
	//		after merge:
	//			the same as before merge
	check = CheckState{
		BeforeRun: uncommitedState{
			Uncommited: []Check{
				{"address", sender, true},
				{"address", coinBase, true},
				{"address", Address1, false},
				{"slot", Address1, key1, false},
			},
		},
		BeforeMerge: uncommitedState{
			Uncommited: []Check{
				{"address", sender, true},
				{"address", coinBase, true},
				{"address", Address1, false},
				{"address", Address2, true},
				{"slot", Address1, key1, false},
				{"slot", Address2, key2, true},
			},
		},
		AfterMerge: []Check{
			{"address", sender, true},
			{"address", coinBase, true},
			{"address", Address1, false},
			{"address", Address2, true},
			{"slot", Address1, key1, false},
			{"slot", Address2, key2, true},
		},
	}
	statedb, maindb = newStateDB(), newStateDB()
	prepare.Call(statedb)
	prepare.Call(maindb)
	statedb.Prepare(params.Rules{IsBerlin: true, IsShanghai: true}, sender, coinBase, nil, nil, nil)
	uncommited = newUncommittedDB(maindb)
	uncommited.Prepare(params.Rules{IsBerlin: true, IsShanghai: true}, sender, coinBase, nil, nil, nil)
	if err := runCase(tx, statedb, uncommited, check); err != nil {
		t.Fatalf("ut failed, err=%s", err.Error())
	}

}

// ================== conflict test ==================
// case 1. conflict in objects: balance, nonce, code, state
// case 2. conflict in transient storage
// case 3. conflict in accesslist
// case 4. conflict in logs
func TestPevmConflictObject(t *testing.T) {
	// case 1. conflict in balance
	// case 2. conflict in nonce
	// case 3. conflict in code
	// case 4. conflict in state

	// case of balance
	prepare := Txs{
		{
			{"AddBalance", Address1, big.NewInt(10)},
		},
	}
	add40 := Txs{
		{
			{"AddBalance", Address1, big.NewInt(30)},
		},
	}
	add50 := Txs{
		{
			{"AddBalance", Address1, big.NewInt(50)},
		},
	}
	check := []Check{
		{"balance", Address1, big.NewInt(40)},
	}
	check2 := []Check{
		{"balance", Address1, big.NewInt(30)},
	}
	if err := runConflictCase(nil, add40, add50, check2); err != nil {
		t.Fatalf("ut failed, errr:%s", err.Error())
	}
	if err := runConflictCase(prepare, add40, add50, check); err != nil {
		t.Fatalf("ut failed, errr:%s", err.Error())
	}

	// case of nonce
	prepare = Txs{
		{
			{"SetNonce", Address1, 10},
		},
	}
	txs := Txs{
		{
			{"SetNonce", Address1, 21},
		},
	}
	txs2 := Txs{
		{
			{"SetNonce", Address1, 22},
		},
	}
	check = []Check{
		{"nonce", Address1, 21},
	}
	if err := runConflictCase(nil, txs, txs2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
	if err := runConflictCase(prepare, txs, txs2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}

	// case of code
	prepare = Txs{
		{
			{"SetCode", Address1, []byte("hello")},
		},
	}
	op1 := Txs{
		{
			{"SetCode", Address1, []byte("here we go")},
		},
	}
	op2 := Txs{
		{
			{"SetCode", Address1, []byte("here we go now")},
		},
	}
	check = []Check{
		{"code", Address1, []byte("here we go")},
	}
	if err := runConflictCase(nil, op1, op2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
	if err := runConflictCase(prepare, op1, op2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}

	// case of state
	prepare = Txs{
		{
			{"SetState", Address1, "key1", "value1"},
			{"SetState", Address1, "key2", "value2"},
		},
	}
	op1 = Txs{
		{
			{"SetState", Address1, "key2", "value2.22"},
			{"SetState", Address1, "key3", "value2.33"},
		},
	}
	op2 = Txs{
		{
			{"SetState", Address1, "key2", "value3.22"},
			{"SetState", Address1, "key1", "value3.11"},
		},
	}
	check = []Check{
		{"state", Address1, "key2", "value3.22"},
		{"state", Address1, "key1", "value3.11"},
		{"state", Address1, "key3", ""},
	}
	if err := runConflictCase(prepare, op2, op1, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
	if err := runConflictCase(nil, op2, op1, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}

}

func TestPevmConflictLogs(t *testing.T) {
	thash1 := common.BytesToHash([]byte("tx1"))
	thash2 := common.BytesToHash([]byte("tx2"))
	thash3 := common.BytesToHash([]byte("tx3"))
	prepare := Txs{
		{
			{"SetTxContext", thash1, 0},
			{"AddLog", &types.Log{Data: []byte("hello")}},
		},
	}
	op1 := Txs{
		{
			{"SetTxContext", thash2, 1},
			{"AddLog", &types.Log{Data: []byte("world")}},
		},
	}
	op2 := Txs{
		{
			{"SetTxContext", thash3, 1},
			{"AddLog", &types.Log{Data: []byte("eth")}},
		},
	}
	check := []Check{
		{"loglen", 2},
		{"log", thash1, 0, []byte("hello"), 0, 0},
		{"log", thash2, 0, []byte("world"), 1, 1},
	}
	if err := runConflictCase(prepare, op1, op2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}

}

func TestPevmConflictPreimage(t *testing.T) {
	op1 := Txs{
		{
			{"AddPreimage", common.BytesToHash([]byte("key1")), []byte("value1")},
		},
	}
	op2 := Txs{
		{
			{"AddPreimage", common.BytesToHash([]byte("key1")), []byte("value2")},
		},
	}
	check := []Check{
		{"preimage", common.BytesToHash([]byte("key1")), []byte("value1")},
	}
	if err := runConflictCase(nil, op1, op2, check); err != nil {
		t.Fatalf("ut failed, err:%s", err.Error())
	}
}

type uncommitedState struct {
	Uncommited []Check
	Maindb     []Check
}

type CheckState struct {
	BeforeRun   uncommitedState
	BeforeMerge uncommitedState
	AfterMerge  []Check
}

type Check []interface{}

type Checks []Check

func (c Checks) Verify(state vm.StateDB) error {
	for _, check := range c {
		if err := check.Verify(state); err != nil {
			return err
		}
	}
	return nil
}

//
//		{"address", Address1, true},
//		{"slot", Address1, "key1", true},

func (c Check) Verify(state vm.StateDB) error {
	switch c[0].(string) {

	case "address":
		addr := c[1].(common.Address)
		exists := c[2].(bool)
		var accesslist *accessList
		if db, ok := state.(*StateDB); ok {
			accesslist = db.accessList
		} else if db, ok := state.(*UncommittedDB); ok {
			accesslist = db.accessList
		} else {
			panic("unknown stateDB type")
		}
		if exists == accesslist.ContainsAddress(addr) {
			return nil
		} else {
			return fmt.Errorf("address 'in list' mismatch, addr:%s, expected:%t, actual:%t", addr.String(), exists, accesslist.ContainsAddress(addr))
		}

	case "slot":
		addr := c[1].(common.Address)
		key := c[2].(common.Hash)
		exists := c[3].(bool)
		var accesslist *accessList
		if db, ok := state.(*StateDB); ok {
			accesslist = db.accessList
		} else if db, ok := state.(*UncommittedDB); ok {
			accesslist = db.accessList
		} else {
			panic("unknown stateDB type")
		}
		_, slotExists := accesslist.Contains(addr, key)
		if exists == slotExists {
			return nil
		} else {
			return fmt.Errorf("slot 'in list' mismatch, addr:%s, key:%s, expected:%t, actual:%t", addr, key.String(), exists, slotExists)
		}

	case "loglen":
		loglen := c[1].(int)
		var logSize int
		if db, ok := state.(*StateDB); ok {
			logSize = int(db.logSize)
		} else if db, ok := state.(*UncommittedDB); ok {
			logSize = len(db.logs)
		} else {
			panic("unknown stateDB type")
		}
		if loglen == logSize {
			return nil
		} else {
			return fmt.Errorf("loglen mismatch, expected:%d, actual:%d", loglen, logSize)
		}

		//{"log", thash, 0, []byte("hello"), 20, 0},
	case "log":
		thash := c[1].(common.Hash)
		i := c[2].(int)
		data := c[3].([]byte)
		txIndex := c[4].(int)
		Index := c[5].(int)
		var logs []*types.Log
		if db, ok := state.(*StateDB); ok {
			logs, ok = db.logs[thash]
			if !ok {
				return fmt.Errorf("log thash not found, thash:%s", thash.String())
			}
		} else if db, ok := state.(*UncommittedDB); ok {
			if thash.Cmp(db.txHash) != 0 {
				return fmt.Errorf("log thash not found, expected:%s, actual:%s", thash.String(), db.txHash.String())
			}
			logs = db.logs
		} else {
			panic("unknown stateDB type")
		}
		if len(logs) <= i {
			return fmt.Errorf("log index out of range, index:%d, len:%d", i, len(logs))
		}
		log := logs[i]
		if !bytes.Equal(log.Data, data) {
			return fmt.Errorf("log mismatch, expected:%s, actual:%s", string(data), string(logs[i].Data))
		}
		if log.TxIndex != uint(txIndex) {
			return fmt.Errorf("log txIndex mismatch, expected:%d, actual:%d", txIndex, log.TxIndex)
		}
		if log.Index != uint(Index) {
			return fmt.Errorf("log index mismatch, expected:%d, actual:%d", Index, log.Index)
		}
		return nil

	case "preimage":
		hash := c[1].(common.Hash)
		data := c[2].([]byte)
		var preimages map[common.Hash][]byte
		if db, ok := state.(*StateDB); ok {
			preimages = db.preimages
		} else if db, ok := state.(*UncommittedDB); ok {
			preimages = db.preimages
		} else {
			panic("unknown stateDB type")
		}
		if bytes.Equal(preimages[hash], data) {
			return nil
		} else {
			return fmt.Errorf("preimage mismatch, expected:%s, actual:%s", string(data), string(preimages[hash]))
		}

	case "preimageExists":
		hash := c[1].(common.Hash)
		exists := c[2].(bool)
		var preimages map[common.Hash][]byte
		if db, ok := state.(*StateDB); ok {
			preimages = db.preimages
		} else if db, ok := state.(*UncommittedDB); ok {
			preimages = db.preimages
		} else {
			panic("unknown stateDB type")
		}
		if _, ok := preimages[hash]; ok == exists {
			return nil
		} else {
			return fmt.Errorf("preimageExists mismatch, expected:%t, actual:%t", exists, ok)
		}

	case "tstorage":
		addr := c[1].(common.Address)
		key := common.BytesToHash([]byte(c[2].(string)))
		val := common.BytesToHash([]byte(c[3].(string)))
		if state.GetTransientState(addr, key).Cmp(val) == 0 {
			return nil
		} else {
			return fmt.Errorf("tstorage mismatch, key:%s, expected:%s, actual:%s", key.String(), val.String(), state.GetTransientState(addr, key).String())
		}

	case "transientExists":
		addr := c[1].(common.Address)
		key := common.BytesToHash([]byte(c[2].(string)))
		exists := c[3].(bool)
		var ts transientStorage
		if db, ok := state.(*StateDB); ok {
			ts = db.transientStorage
		} else if db, ok := state.(*UncommittedDB); ok {
			ts = db.transientStorage
		} else {
			panic("unknown stateDB type")
		}
		found := false
		if _, ok := ts[addr]; ok {
			if _, ok := ts[addr][key]; ok {
				found = true
			}
		}
		if found == exists {
			return nil
		} else {
			return fmt.Errorf("transientExists mismatch, expected:%t, actual:%t", exists, found)
		}

	case "exists":
		addr := c[1].(common.Address)
		exists := c[2].(bool)
		if state.Exist(addr) == exists {
			return nil
		} else {
			return fmt.Errorf("exists mismatch,addr:%s, expected:%t, actual:%t", addr.String(), exists, state.Exist(addr))
		}

	case "empty":
		addr := c[1].(common.Address)
		empty := c[2].(bool)
		if state.Empty(addr) == empty {
			return nil
		} else {
			return fmt.Errorf("empty mismatch, expected:%t, actual:%t", empty, state.Empty(addr))
		}

	case "balance":
		addr := c[1].(common.Address)
		balance, _ := uint256.FromBig(c[2].(*big.Int))
		if state.GetBalance(addr).Cmp(balance) == 0 {
			return nil
		} else {
			return fmt.Errorf("balance mismatch, expected:%d, actual:%d", balance.Uint64(), state.GetBalance(addr).Uint64())
		}

	case "nonce":
		addr := c[1].(common.Address)
		nonce := uint64(c[2].(int))
		if state.GetNonce(addr) == nonce {
			return nil
		} else {
			return fmt.Errorf("nonce mismatch, expected:%d, actual:%d", nonce, state.GetNonce(addr))
		}

	case "code":
		addr := c[1].(common.Address)
		code := c[2].([]byte)
		if bytes.Equal(state.GetCode(addr), code) {
			return nil
		} else {
			return fmt.Errorf("code mismatch, expected:%s, actual:%s", string(code), string(state.GetCode(addr)))
		}

	case "codeHash":
		addr := c[1].(common.Address)
		codeHash := c[2].(common.Hash)
		if codeHash.Cmp(state.GetCodeHash(addr)) == 0 {
			return nil
		} else {
			return fmt.Errorf("codeHash mismatch, expected:%s, actual:%s", codeHash.String(), state.GetCodeHash(addr).String())
		}

	case "state":
		addr := c[1].(common.Address)
		key := common.BytesToHash([]byte(c[2].(string)))
		val := common.BytesToHash([]byte(c[3].(string)))
		if state.GetState(addr, key).Cmp(val) == 0 {
			return nil
		} else {
			return fmt.Errorf("state mismatch, key:%s, expected:%s, actual:%s", key.String(), val.String(), state.GetState(addr, key).String())
		}

	case "cstate":
		addr := c[1].(common.Address)
		key := common.BytesToHash([]byte(c[2].(string)))
		val := common.BytesToHash([]byte(c[3].(string)))
		if state.GetCommittedState(addr, key).Cmp(val) == 0 {
			return nil
		} else {
			return fmt.Errorf("committed state mismatch, expected:%s, actual:%s", val.String(), state.GetCommittedState(addr, key).String())
		}

	case "obj":
		addr := c[1].(common.Address)
		exists := c[2].(string)
		if exists == "nil" {
			if state.Exist(addr) {
				return fmt.Errorf("object was expected not exists, addr:%s", addr.String())
			} else {
				return nil
			}
		} else {
			if !state.Exist(addr) {
				return fmt.Errorf("object was expected exists, addr:%s", addr.String())
			} else {
				return nil
			}
		}

	case "refund":
		refund := uint64(c[1].(int))
		if state.GetRefund() == refund {
			return nil
		} else {
			return fmt.Errorf("refund mismatch, expected:%d, actual:%d", refund, state.GetRefund())
		}

	default:
		panic(fmt.Sprintf("unknown check type: %s", c[0].(string)))
	}
}

type Op []interface{}

type Tx []Op

func (op Op) Call(db vm.StateDB) error {
	switch op[0].(string) {
	case "GetNonce":
		addr := op[1].(common.Address)
		db.GetNonce(addr)
		return nil
	case "GetCodeHash":
		addr := op[1].(common.Address)
		db.GetCodeHash(addr)
		return nil
	case "SetTxContext":
		state := db
		if db, ok := state.(*UncommittedDB); ok {
			db.SetTxContext(op[1].(common.Hash), op[2].(int))
		} else if db, ok := state.(*StateDB); ok {
			db.SetTxContext(op[1].(common.Hash), op[2].(int))
		} else {
			panic("unknown stateDB type")
		}
		return nil

	case "AddSlots":
		addr := op[1].(common.Address)
		slot := op[2].(common.Hash)
		db.AddSlotToAccessList(addr, slot)
		return nil

	case "AddAddress":
		addr := op[1].(common.Address)
		db.AddAddressToAccessList(addr)
		return nil

	case "AddLog":
		log := op[1].(*types.Log)
		db.AddLog(log)
		return nil

	case "SetTransientStorage":
		addr := op[1].(common.Address)
		key := op[2].(string)
		val := op[3].(string)
		db.SetTransientState(addr, common.BytesToHash([]byte(key)), common.BytesToHash([]byte(val)))
		return nil

	case "Create":
		addr := op[1].(common.Address)
		db.CreateAccount(addr)
		return nil

	case "AddPreimage":
		hash := op[1].(common.Hash)
		data := op[2].([]byte)
		db.AddPreimage(hash, data)
		return nil

	case "AddBalance":
		addr := op[1].(common.Address)
		balance, _ := uint256.FromBig(op[2].(*big.Int))
		db.AddBalance(addr, balance)
		return nil
	case "SubBalance":
		addr := op[1].(common.Address)
		balance, _ := uint256.FromBig(op[2].(*big.Int))
		db.SubBalance(addr, balance)
		return nil

	case "SetNonce":
		addr := op[1].(common.Address)
		nonce := uint64(op[2].(int))
		db.SetNonce(addr, nonce)
		return nil

	case "SetCode":
		addr := op[1].(common.Address)
		code := op[2].([]byte)
		db.SetCode(addr, code)
		return nil

	case "AddRefund":
		refund := uint64(op[1].(int))
		db.AddRefund(refund)
		return nil

	case "SubRefund":
		refund := uint64(op[1].(int))
		db.SubRefund(refund)
		return nil

	case "SetState":
		addr := op[1].(common.Address)
		key := common.BytesToHash([]byte(op[2].(string)))
		val := common.BytesToHash([]byte(op[3].(string)))
		db.SetState(addr, key, val)
		return nil

	case "SelfDestruct":
		addr := op[1].(common.Address)
		db.SelfDestruct(addr)
		return nil

	case "SelfDestruct6780":
		addr := op[1].(common.Address)
		db.Selfdestruct6780(addr)
		return nil

	case "Snapshot":
		snapid := op[1].(*int)
		*snapid = db.Snapshot()
		return nil

	case "Revert":
		snapid := op[1].(*int)
		db.RevertToSnapshot(*snapid)
		return nil

	default:
		return fmt.Errorf("unknown op type: %s", op[0].(string))
	}
}

// Call executes the transaction.
func (tx Tx) Call(db vm.StateDB) error {
	for _, op := range tx {
		if err := op.Call(db); err != nil {
			return err
		}
	}
	return nil
}

type Txs []Tx

func (txs Txs) Call(db vm.StateDB) error {
	for _, tx := range txs {
		if err := tx.Call(db); err != nil {
			return err
		}
	}
	return nil
}

func triedbConfig(StateScheme string) *triedb.Config {
	config := &triedb.Config{
		Preimages: true,
		NoTries:   false,
	}
	if StateScheme == rawdb.HashScheme {
		config.HashDB = &hashdb.Config{
			CleanCacheSize: 1 * 1024 * 1024,
		}
	}
	if StateScheme == rawdb.PathScheme {
		config.PathDB = &pathdb.Config{
			//TrieNodeBufferType:   c.PathNodeBuffer,
			//StateHistory:         c.StateHistory,
			CleanCacheSize: 1 * 1024 * 1024,
			DirtyCacheSize: 1 * 1024 * 1024,
			//ProposeBlockInterval: c.ProposeBlockInterval,
			//NotifyKeep:           keepFunc,
			//JournalFilePath:      c.JournalFilePath,
			//JournalFile:          c.JournalFile,
		}
	}
	return config
}

func newStateDB() *StateDB {
	memdb := rawdb.NewMemoryDatabase()
	// Open trie database with provided config
	triedb := triedb.NewDatabase(memdb, triedbConfig(rawdb.HashScheme))
	stateCache := NewDatabaseWithNodeDB(memdb, triedb)
	st, err := New(common.Hash{}, stateCache, nil)
	if err != nil {
		panic(err)
	}
	return st
}

func newUncommittedDB(db *StateDB) *UncommittedDB {
	return NewUncommittedDB(db)
}

func runTxsOnStateDB(txs Txs, db *StateDB, check CheckState) (common.Hash, error) {
	// run the transactions
	if err := txs.Call(db); err != nil {
		return common.Hash{}, fmt.Errorf("state failed to run txs: %v", err)
	}
	// states before merge should be the same as the uncommitted db
	for _, c := range check.BeforeMerge.Uncommited {
		if err := c.Verify(db); err != nil {
			return common.Hash{}, fmt.Errorf("[before merge][statedbt db] failed to verify : %v", err)
		}
	}
	return db.IntermediateRoot(true), nil
}

func runTxOnUncommittedDB(txs Txs, db *UncommittedDB, check CheckState) (common.Hash, error) {
	// run the transaction
	if err := txs.Call(db); err != nil {
		return common.Hash{}, fmt.Errorf("unconfirm db failed to run txs: %v", err)
	}
	for _, check := range check.BeforeMerge.Uncommited {
		if err := check.Verify(db); err != nil {
			return common.Hash{}, fmt.Errorf("[before merge][uncommited db] failed to verify : %v", err)
		}
	}
	for _, check := range check.BeforeMerge.Maindb {
		if err := check.Verify(db.maindb); err != nil {
			return common.Hash{}, fmt.Errorf("[before merge][maindb] failed to verify : %v", err)
		}
	}
	if err := db.Merge(true); err != nil {
		return common.Hash{}, fmt.Errorf("failed to merge: %v", err)
	}
	return db.maindb.IntermediateRoot(true), nil
}

func runConflictCase(prepare, txs1, txs2 Txs, checks []Check) error {
	maindb := newStateDB()
	if prepare != nil {
		maindb.CreateAccount(Address1)
		for _, op := range prepare {
			if err := op.Call(maindb); err != nil {
				return fmt.Errorf("failed to call prepare txs, err:%s", err.Error())
			}
		}
	}
	un1, un2 := newUncommittedDB(maindb), newUncommittedDB(maindb)
	if err := txs1.Call(un1); err != nil {
		return fmt.Errorf("failed to call txs1, err:%s", err.Error())
	}
	if err := txs2.Call(un2); err != nil {
		return fmt.Errorf("failed to call txs2, err:%s", err.Error())
	}
	if err := un1.ConflictsToMaindb(); err != nil {
		return fmt.Errorf("failed to check conflicts of un1, err:%s", err.Error())
	}
	if err := un1.Merge(true); err != nil {
		return fmt.Errorf("failed to merge un1, err:%s", err.Error())
	}
	if err := un2.ConflictsToMaindb(); err == nil {
		return fmt.Errorf("un2 merge is expected to be failed")
	}
	for _, c := range checks {
		if err := c.Verify(maindb); err != nil {
			return fmt.Errorf("failed to verify maindb, err:%s", err.Error())
		}
	}
	return nil
}

func runCase(txs Txs, state *StateDB, unstate *UncommittedDB, check CheckState) error {
	stRoot, errSt := runTxsOnStateDB(txs, state, check)
	unRoot, errUn := runTxOnUncommittedDB(txs, unstate, check)
	if errSt != nil {
		return fmt.Errorf("failed to run txs on state db: %v", errSt)
	}
	if errUn != nil {
		return fmt.Errorf("failed to run tx on uncommited db: %v", errUn)
	}
	for _, check := range check.AfterMerge {
		if err := check.Verify(state); err != nil {
			return fmt.Errorf("[after merge] failed to verify statedb: %v", err)
		}
		// an uncommitted is invalid after merge, so we verify its maindb instead
		if err := check.Verify(unstate.maindb); err != nil {
			return fmt.Errorf("[after merge] failed to verify uncommited db: %v", err)
		}
	}
	if stRoot.Cmp(unRoot) != 0 {
		return fmt.Errorf("state root mismatch: %s != %s", stRoot.String(), unRoot.String())
	}
	return nil
}

func Diff(a, b *StateDB) []common.Hash {
	// compare the two stateDBs, and return the different objects
	return nil
}

type object struct {
	data int
}

type HashSyncMap struct {
	cap  int
	maps []sync.Map
}

func newHashSyncMap(cap int) *HashSyncMap {
	hsm := &HashSyncMap{cap: cap, maps: make([]sync.Map, cap)}
	return hsm
}

func (hsm *HashSyncMap) Load(key interface{}) (value any, ok bool) {
	addr := key.(common.Address)
	slot := hash2int(addr) % hsm.cap
	return hsm.maps[slot].Load(key)
}

func (hsm *HashSyncMap) Store(key, val interface{}) {
	addr := key.(common.Address)
	slot := hash2int(addr) % hsm.cap
	hsm.maps[slot].Store(key, val)
}

type HashMutexMap struct {
	cap  int
	maps []struct {
		sync.Mutex
		data map[common.Address]*object
	}
}

func newHashMutexMap(cap int) *HashMutexMap {
	hmm := &HashMutexMap{cap: cap, maps: make([]struct {
		sync.Mutex
		data map[common.Address]*object
	}, cap)}
	for i := 0; i < cap; i++ {
		hmm.maps[i].data = make(map[common.Address]*object)
	}
	return hmm
}

func (hmm *HashMutexMap) Load(key interface{}) (value any, ok bool) {
	addr := key.(common.Address)
	slot := hash2int(addr) % hmm.cap
	cache := &hmm.maps[slot]
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	value, ok = cache.data[addr]
	return value, ok
}

func (hmm *HashMutexMap) Store(key, val interface{}) {
	addr := key.(common.Address)
	obj := val.(*object)
	slot := hash2int(addr) % hmm.cap
	cache := &hmm.maps[slot]
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	cache.data[addr] = obj
}

type istore interface {
	Store(key, val interface{})
	Load(key interface{}) (value any, ok bool)
}

type DB struct {
	cache   istore
	persist map[common.Address]*object
}

func newDB(cache istore, addresses []common.Address) *DB {
	db := &DB{cache: cache, persist: make(map[common.Address]*object)}
	i := 0
	for _, addr := range addresses {
		db.persist[addr] = &object{data: i}
		i++
	}
	return db
}

func (db *DB) Get(addr common.Address) *object {
	if obj, ok := db.cache.Load(addr); ok {
		return obj.(*object)
	}
	obj := db.persist[addr]
	db.cache.Store(addr, obj)
	return obj
}

func hash2int(addr common.Address) int {
	return int(addr[common.AddressLength/2])
}

func randAddresses(num int) []common.Address {
	addresses := make([]common.Address, num)
	for i := 0; i < len(addresses); i++ {
		addr, _ := randAddress()
		addresses[i] = addr
	}
	return addresses
}

func randAddress() (common.Address, *ecdsa.PrivateKey) {
	// Generate a new private key using rand.Reader
	key, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	return crypto.PubkeyToAddress(key.PublicKey), key
}

func accessWithMultiProcesses(db *DB, parallelNum int, queryNum int, randAddress []common.Address) {
	wait := sync.WaitGroup{}
	wait.Add(parallelNum)
	for p := 0; p < parallelNum; p++ {
		runner <- func() {
			for j := 0; j < queryNum; j++ {
				addr := randAddress[j%10000]
				obj := db.Get(addr)
				if obj != nil {
					obj.data++
				}
				db.cache.Store(addr, obj)
			}
			wait.Done()
		}
	}
	wait.Wait()
}

var runner = make(chan func(), runtime.NumCPU())

func init() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for f := range runner {
				f()
			}
		}()
	}
}

func BenchmarkSyncMap10000(b *testing.B) {
	sm := sync.Map{}
	runBench(&sm, b)
}

func BenchmarkHashSyncMap10000(b *testing.B) {
	sm := newHashSyncMap(128)
	runBench(sm, b)
}

func BenchmarkHashMutexMap10000(b *testing.B) {
	sm := newHashMutexMap(128)
	runBench(sm, b)
}

func runBench(sm istore, b *testing.B) {
	randAddress := randAddresses(10000)
	db := newDB(sm, randAddress)
	b.ResetTimer()
	var duration int64 = 0
	var round int64 = 0
	for i := 0; i < b.N; i++ {
		start := time.Now()
		accessWithMultiProcesses(db, runtime.NumCPU(), 10000, randAddress)
		atomic.AddInt64(&duration, int64(time.Since(start)))
		atomic.AddInt64(&round, 1)
	}
	fmt.Printf("total cost:%s, rounds:%d, avg costs:%s\n", time.Duration(duration), round, time.Duration(duration/round))
}
