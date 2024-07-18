package state

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/holiman/uint256"
	"runtime"
	"sort"
	"sync"
	"time"
)

const defaultNumOfSlots = 100

var parallelKvOnce sync.Once

type ParallelKvCheckUnit struct {
	addr common.Address
	key  common.Hash
	val  common.Hash
}

type ParallelKvCheckMessage struct {
	slotDB   *ParallelStateDB
	isStage2 bool
	kvUnit   ParallelKvCheckUnit
}

var parallelKvCheckReqCh chan ParallelKvCheckMessage
var parallelKvCheckResCh chan bool

type ParallelStateDB struct {
	StateDB
}

func (s *ParallelStateDB) GetRefund() uint64 {
	return s.refund
}

func (s *ParallelStateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.ContainsAddress(addr)
}

func (s *ParallelStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	return s.accessList.Contains(addr, slot)
}

func (s *ParallelStateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.AddAddress(addr) {
		s.journal.append(accessListAddAccountChange{&addr})
	}
}

func (s *ParallelStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrMod, slotMod := s.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		s.journal.append(accessListAddAccountChange{&addr})
	}
	if slotMod {
		s.journal.append(accessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
}

func (s *ParallelStateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

func hasKvConflict(slotDB *ParallelStateDB, addr common.Address, key common.Hash, val common.Hash, isStage2 bool) bool {
	mainDB := slotDB.parallel.baseStateDB

	if isStage2 { // update slotDB's unconfirmed DB list and try
		if valUnconfirm, ok := slotDB.getKVFromUnconfirmedDB(addr, key); ok {
			if !bytes.Equal(val.Bytes(), valUnconfirm.Bytes()) {
				log.Debug("IsSlotDBReadsValid KV read is invalid in unconfirmed", "addr", addr,
					"valSlot", val, "valUnconfirm", valUnconfirm,
					"SlotIndex", slotDB.parallel.SlotIndex,
					"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
				return true
			}
		}
	}
	valMain := mainDB.GetState(addr, key)
	if !bytes.Equal(val.Bytes(), valMain.Bytes()) {
		log.Debug("hasKvConflict is invalid", "addr", addr,
			"key", key, "valSlot", val,
			"valMain", valMain, "SlotIndex", slotDB.parallel.SlotIndex,
			"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
		return true // return false, Range will be terminated.
	}
	return false
}

// StartKvCheckLoop start several routines to do conflict check
func StartKvCheckLoop() {
	parallelKvCheckReqCh = make(chan ParallelKvCheckMessage, 200)
	parallelKvCheckResCh = make(chan bool, 10)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				kvEle1 := <-parallelKvCheckReqCh
				parallelKvCheckResCh <- hasKvConflict(kvEle1.slotDB, kvEle1.kvUnit.addr,
					kvEle1.kvUnit.key, kvEle1.kvUnit.val, kvEle1.isStage2)
			}
		}()
	}
}

// NewSlotDB creates a new State DB based on the provided StateDB.
// With parallel, each execution slot would have its own StateDB.
// This method must be called after the baseDB call PrepareParallel()
func NewSlotDB(db *StateDB, txIndex int, baseTxIndex int, unconfirmedDBs *sync.Map /*map[int]*ParallelStateDB*/) *ParallelStateDB {
	slotDB := db.CopyForSlot()
	slotDB.txIndex = txIndex
	slotDB.originalRoot = db.originalRoot
	slotDB.parallel.baseStateDB = db
	slotDB.parallel.baseTxIndex = baseTxIndex
	slotDB.parallel.unconfirmedDBs = unconfirmedDBs

	return slotDB
}

// RevertSlotDB keep the Read list for conflict detect,
// discard all state changes except:
//   - nonce and balance of from address
//   - balance of system address: will be used on merge to update SystemAddress's balance
func (s *ParallelStateDB) RevertSlotDB(from common.Address) {
	s.parallel.kvChangesInSlot = make(map[common.Address]StateKeys)
	s.parallel.nonceChangesInSlot = make(map[common.Address]struct{})
	s.parallel.balanceChangesInSlot = make(map[common.Address]struct{}, 1)
	s.parallel.addrStateChangesInSlot = make(map[common.Address]bool) // 0: created, 1: deleted

	selfStateObject := s.parallel.dirtiedStateObjectsInSlot[from]
	s.parallel.dirtiedStateObjectsInSlot = make(map[common.Address]*stateObject, 2)
	// keep these elements
	s.parallel.dirtiedStateObjectsInSlot[from] = selfStateObject
	s.parallel.balanceChangesInSlot[from] = struct{}{}
	s.parallel.nonceChangesInSlot[from] = struct{}{}
}

func (s *ParallelStateDB) getBaseStateDB() *StateDB {
	return &s.StateDB
}

func (s *ParallelStateDB) SetSlotIndex(index int) {
	s.parallel.SlotIndex = index
}

// for parallel execution mode, try to get dirty StateObject in slot first.
// it is mainly used by journal revert right now.
func (s *ParallelStateDB) getStateObject(addr common.Address) *stateObject {
	var object *stateObject
	if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if obj.deleted {
			return nil
		}
		object = obj
	} else {
		// can not call s.StateDB.getStateObject(), since `newObject` need ParallelStateDB as the interface
		object = s.getStateObjectNoSlot(addr)
	}
	return object
}

func (s *ParallelStateDB) storeStateObj(addr common.Address, stateObject *stateObject) {
	// When a state object is stored into s.parallel.stateObjects,
	// it belongs to base StateDB, it is confirmed and valid.
	// todo Dav: why need change this? -- delete me !
	// stateObject.db = s.parallel.baseStateDB
	// stateObject.dbItf = s.parallel.baseStateDB

	// the object could be created in SlotDB, if it got the object from DB and
	// update it to the shared `s.parallel.stateObjects``
	stateObject.db.parallelStateAccessLock.Lock()
	if _, ok := s.parallel.stateObjects.Load(addr); !ok {
		s.parallel.stateObjects.Store(addr, stateObject)
	}
	stateObject.db.parallelStateAccessLock.Unlock()
}

func (s *ParallelStateDB) getStateObjectNoSlot(addr common.Address) *stateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.

// prev is used for CreateAccount to get its balance
// Parallel mode:
// if prev in dirty:  revert is ok
// if prev in unconfirmed DB:  addr state read record, revert should not put it back
// if prev in main DB:  addr state read record, revert should not put it back
// if pre no exist:  addr state read record,

// `prev` is used to handle revert, to recover with the `prev` object
// In Parallel mode, we only need to recover to `prev` in SlotDB,
//
//	a.if it is not in SlotDB, `revert` will remove it from the SlotDB
//	b.if it is existed in SlotDB, `revert` will recover to the `prev` in SlotDB
//	c.as `snapDestructs` it is the same
func (s *ParallelStateDB) createObject(addr common.Address) (newobj *stateObject) {
	prev := s.parallel.dirtiedStateObjectsInSlot[addr]
	// TODO-dav: check
	// There can be tx0 create an obj at addr0, tx1 destruct it, and tx2 recreate it use create2.
	// so if tx0 is finalized, and tx1 is unconfirmed, we have to check the states of unconfirmed, otherwise there
	// will be wrong behavior that we recreate an object that is already there. see. test "TestDeleteThenCreate"
	var prevdestruct bool

	if s.snap != nil && prev != nil {
		s.snapParallelLock.Lock()
		_, prevdestruct = s.snapDestructs[prev.address]
		s.parallel.addrSnapDestructsReadsInSlot[addr] = prevdestruct
		if !prevdestruct {
			// To destroy the previous trie node first and update the trie tree
			// with the new object on block commit.
			s.snapDestructs[prev.address] = struct{}{}
		}
		s.snapParallelLock.Unlock()
	}
	newobj = newObject(s, s.isParallel, addr, nil)
	newobj.setNonce(0) // sets the object to dirty
	if prev == nil {
		s.journal.append(createObjectChange{account: &addr})
	} else {
		s.journal.append(resetObjectChange{prev: prev, prevdestruct: prevdestruct})
	}

	s.parallel.addrStateChangesInSlot[addr] = true // the object is created
	s.parallel.nonceChangesInSlot[addr] = struct{}{}
	s.parallel.balanceChangesInSlot[addr] = struct{}{}
	s.parallel.codeChangesInSlot[addr] = struct{}{}
	// notice: all the KVs are cleared if any
	s.parallel.kvChangesInSlot[addr] = make(StateKeys)
	newobj.created = true
	s.parallel.dirtiedStateObjectsInSlot[addr] = newobj
	return newobj
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *ParallelStateDB) getDeletedStateObject(addr common.Address) *stateObject {

	// Prefer live objects if any is available
	if obj, _ := s.getStateObjectFromStateObjects(addr); obj != nil {
		return obj
	}

	data, ok := s.getStateObjectFromSnapshotOrTrie(addr)
	if !ok {
		return nil
	}

	// this is why we have to use a separate getDeletedStateObject for ParallelStateDB
	// `s` has to be the ParallelStateDB
	obj := newObject(s, s.isParallel, addr, data)
	s.storeStateObj(addr, obj)
	return obj
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
// dirtyInSlot -> Unconfirmed DB -> main DB -> snapshot, no? create one
func (s *ParallelStateDB) GetOrNewStateObject(addr common.Address) *stateObject {
	var object *stateObject
	var ok bool
	if object, ok = s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		return object
	}

	// try unconfirmedDB
	object, _ = s.getStateObjectFromUnconfirmedDB(addr)
	if object != nil {
		// object found in unconfirmedDB, check existence
		if object.deleted || object.selfDestructed {
			object = s.createObject(addr)
			s.parallel.addrStateReadsInSlot[addr] = false
			return object
		}
	} else {
		object = s.getStateObjectNoSlot(addr) // try to get from base db
	}
	// not found, or found in NoSlot or found in unconfirmed.
	exist := true
	// TODO-dav: the check of nil and delete already done by NoSlot and unconfirmedDB, may optimize it for dirty only.
	if object == nil || object.deleted {
		object = s.createObject(addr)
		exist = false
	}
	s.parallel.addrStateReadsInSlot[addr] = exist // true: exist, false: not exist
	return object
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *ParallelStateDB) Exist(addr common.Address) bool {
	// 1.Try to get from dirty
	if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if obj.deleted {
			log.Error("Exist in dirty, but marked as deleted or suicided",
				"txIndex", s.txIndex, "baseTxIndex:", s.parallel.baseTxIndex)
			return false
		}
		return true
	}
	// 2.Try to get from unconfirmed & main DB
	// 2.1 Already read before
	if exist, ok := s.parallel.addrStateReadsInSlot[addr]; ok {
		return exist
	}

	// 2.2 Try to get from unconfirmed DB if exist
	if exist, ok := s.getAddrStateFromUnconfirmedDB(addr, false); ok {
		s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
		return exist
	}

	// 3.Try to get from main StateDB
	exist := s.getStateObjectNoSlot(addr) != nil
	s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
	return exist
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *ParallelStateDB) Empty(addr common.Address) bool {
	// 1.Try to get from dirty
	if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		// dirty object is light copied and fixup on need,
		// empty could be wrong, except it is created with this TX
		if _, ok := s.parallel.addrStateChangesInSlot[addr]; ok {
			return obj.empty()
		}
		// so we have to check it manually
		// empty means: Nonce == 0 && Balance == 0 && CodeHash == emptyCodeHash
		if s.GetBalance(addr).Sign() != 0 { // check balance first, since it is most likely not zero
			return false
		}
		if s.GetNonce(addr) != 0 {
			return false
		}
		codeHash := s.GetCodeHash(addr)
		return bytes.Equal(codeHash.Bytes(), emptyCodeHash) // code is empty, the object is empty
	}
	// 2.Try to get from unconfirmed & main DB
	// 2.1 Already read before
	if exist, ok := s.parallel.addrStateReadsInSlot[addr]; ok {
		// exist means not empty
		return !exist
	}
	// 2.2 Try to get from unconfirmed DB if exist
	if exist, ok := s.getAddrStateFromUnconfirmedDB(addr, true); ok {
		s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
		return !exist
	}
	// 2.3 Try to get from NoSlot.
	so := s.getStateObjectNoSlot(addr)
	exist := so != nil
	empty := (!exist) || so.empty()

	s.parallel.addrStateReadsInSlot[addr] = exist // update read cache
	return empty
}

// GetBalance retrieves the balance from the given address or 0 if object not found
// GetFrom the dirty list => from unconfirmed DB => get from main stateDB
func (s *ParallelStateDB) GetBalance(addr common.Address) *uint256.Int {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o.deleted {
			return common.U2560
		}
		dirtyObj = o
	}

	// 1.Try to get from dirty
	if _, ok := s.parallel.balanceChangesInSlot[addr]; ok {
		// on balance fixup, addr may not exist in dirtiedStateObjectsInSlot
		// we intend to fixup balance based on unconfirmed DB or main DB
		return dirtyObj.Balance()
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if balance, ok := s.parallel.balanceReadsInSlot[addr]; ok {
		return balance
	}

	balance := common.U2560
	// 2.2 Try to get from unconfirmed DB if exist
	if blc := s.getBalanceFromUnconfirmedDB(addr); blc != nil {
		balance = blc
	} else {
		// 3. Try to get from main StateObject
		blc = common.U2560
		object := s.getStateObjectNoSlot(addr)
		if object != nil {
			blc = object.Balance()
		}
		balance = blc
	}
	s.parallel.balanceReadsInSlot[addr] = balance

	// fixup dirties
	if dirtyObj != nil && dirtyObj.Balance() != balance {
		dirtyObj.setBalance(balance)
	}

	return balance
}

func (s *ParallelStateDB) GetNonce(addr common.Address) uint64 {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o.deleted {
			return 0
		}
		dirtyObj = o
	}

	// 1.Try to get from dirty
	if _, ok := s.parallel.nonceChangesInSlot[addr]; ok {
		// on nonce fixup, addr may not exist in dirtiedStateObjectsInSlot
		// we intend to fixup nonce based on unconfirmed DB or main DB
		return dirtyObj.Nonce()
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if nonce, ok := s.parallel.nonceReadsInSlot[addr]; ok {
		return nonce
	}

	var nonce uint64 = 0
	// 2.2 Try to get from unconfirmed DB if exist
	if nc, ok := s.getNonceFromUnconfirmedDB(addr); ok {
		nonce = nc
	} else {
		// 3.Try to get from main StateDB
		nc = 0
		object := s.getStateObjectNoSlot(addr)
		if object != nil {
			nc = object.Nonce()
		}
		nonce = nc
	}
	s.parallel.nonceReadsInSlot[addr] = nonce

	// fixup dirties
	if dirtyObj != nil && dirtyObj.Nonce() < nonce {
		dirtyObj.setNonce(nonce)
	}
	return nonce
}

func (s *ParallelStateDB) GetCode(addr common.Address) []byte {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o.deleted {
			return nil
		}
		dirtyObj = o
	}

	// 1.Try to get from dirty
	if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
		// on code fixup, addr may not exist in dirtiedStateObjectsInSlot
		// we intend to fixup code based on unconfirmed DB or main DB
		return dirtyObj.Code()
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if code, ok := s.parallel.codeReadsInSlot[addr]; ok {
		return code
	}
	var code []byte
	// 2.2 Try to get from unconfirmed DB if exist
	if cd, ok := s.getCodeFromUnconfirmedDB(addr); ok {
		code = cd
	} else {
		// 3. Try to get from main StateObject
		object := s.getStateObjectNoSlot(addr)
		if object != nil {
			code = object.Code()
		}
	}
	s.parallel.codeReadsInSlot[addr] = code

	// fixup dirties
	if dirtyObj != nil && !bytes.Equal(dirtyObj.code, code) {
		dirtyObj.code = code
	}
	return code
}

func (s *ParallelStateDB) GetCodeSize(addr common.Address) int {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o.deleted {
			return 0
		}
		dirtyObj = o
	}
	// 1.Try to get from dirty
	if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
		// on code fixup, addr may not exist in dirtiedStateObjectsInSlot
		// we intend to fixup code based on unconfirmed DB or main DB
		return dirtyObj.CodeSize()
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if code, ok := s.parallel.codeReadsInSlot[addr]; ok {
		return len(code) // len(nil) is 0 too
	}

	cs := 0
	var code []byte
	// 2.2 Try to get from unconfirmed DB if exist
	if cd, ok := s.getCodeFromUnconfirmedDB(addr); ok {
		cs = len(cd) // len(nil) is 0 too
		code = cd
	} else {
		// 3. Try to get from main StateObject
		var cc []byte
		object := s.getStateObjectNoSlot(addr)
		if object != nil {
			// This is where we update the code from possible db.ContractCode if the original object.code is nil.
			cc = object.Code()
			cs = object.CodeSize()
		}
		code = cc
	}
	s.parallel.codeReadsInSlot[addr] = code
	// fixup dirties
	if dirtyObj != nil {
		if !bytes.Equal(dirtyObj.code, code) {
			dirtyObj.code = code
		}
	}
	return cs
}

// GetCodeHash return:
//   - common.Hash{}: the address does not exist
//   - emptyCodeHash: the address exist, but code is empty
//   - others:        the address exist, and code is not empty
func (s *ParallelStateDB) GetCodeHash(addr common.Address) common.Hash {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o.deleted {
			return common.Hash{}
		}
		dirtyObj = o
	}

	// 1.Try to get from dirty
	if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
		// on code fixup, addr may not exist in dirtiedStateObjectsInSlot
		// we intend to fixup balance based on unconfirmed DB or main DB
		return common.BytesToHash(dirtyObj.CodeHash())
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if codeHash, ok := s.parallel.codeHashReadsInSlot[addr]; ok {
		return codeHash
	}
	codeHash := common.Hash{}
	// 2.2 Try to get from unconfirmed DB if exist
	if cHash, ok := s.getCodeHashFromUnconfirmedDB(addr); ok {
		codeHash = cHash
	} else {
		// 3. Try to get from main StateObject
		object := s.getStateObjectNoSlot(addr)

		if object != nil {
			codeHash = common.BytesToHash(object.CodeHash())
		}
	}
	s.parallel.codeHashReadsInSlot[addr] = codeHash

	// fill slots in dirty if exist.
	// A case for this:
	// TX0: createAccount at addr 0x123, set code and codehash
	// TX1: AddBalance - now an obj in dirty with empty codehash, and codeChangesInSlot is false (not changed)
	//      GetCodeHash - get from unconfirmedDB or mainDB, set codeHashReadsInSlot to the new val.
	//      SELFDESTRUCT - set codeChangesInSlot, but the obj in dirty is with Empty codehash.
	//     				   obj marked selfdestructed but not deleted. so CodeHash is not empty.
	//      GetCodeHash - since the codeChangesInslot is marked, get the object from dirty, and get the
	//                    wrong 'empty' hash.
	if dirtyObj != nil {
		// found one
		if dirtyObj.CodeHash() == nil || bytes.Equal(dirtyObj.CodeHash(), emptyCodeHash) {
			dirtyObj.data.CodeHash = codeHash.Bytes()
		}
	}
	return codeHash
}

// GetState retrieves a value from the given account's storage trie.
// For parallel mode wih, get from the state in order:
//
//	-> self dirty, both Slot & MainProcessor
//	-> pending of self: Slot on merge
//	-> pending of unconfirmed DB
//	-> pending of main StateDB
//	-> origin
func (s *ParallelStateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	var dirtyObj *stateObject
	// 0. Test whether it is deleted in dirty.
	if o, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if o == nil || o.deleted {
			return common.Hash{}
		}
		dirtyObj = o
	}
	// 1.Try to get from dirty
	if exist, ok := s.parallel.addrStateChangesInSlot[addr]; ok {
		if !exist {
			// it could be suicided within this SlotDB?
			// it should be able to get state from suicided address within a Tx:
			// e.g. within a transaction: call addr:suicide -> get state: should be ok
			// return common.Hash{}
			log.Info("ParallelStateDB GetState suicided", "addr", addr, "hash", hash)
		} else {
			// It is possible that an object get created but not dirtied since there is no state set, such as recreate.
			// In this case, simply return common.Hash{}.
			// This is for corner case:
			//	B0: TX0 --> createAccount @addr1	-- merged into DB
			//  B1: Tx1 and Tx2
			//      Tx1 account@addr1 selfDestruct  -- unconfirmed
			//      Tx2 recreate account@addr2  	-- executing
			// Since any state change and suicide could record in s.parallel.addrStateChangeInSlot, it is save to simple
			// return common.Hash{} for this case as the previous TX must has the object destructed.
			// P.S. if the Tx2 both destruct and recreate the object, it will not fall into this logic, as the change
			// will be recorded in dirtiedStateObjectsInSlot.

			// it could be suicided within this SlotDB?
			// it should be able to get state from suicided address within a Tx:
			// e.g. within a transaction: call addr:suicide -> get state: should be ok
			// return common.Hash{}
			log.Info("ParallelStateDB GetState suicided", "addr", addr, "hash", hash)

			if dirtyObj == nil {
				log.Error("ParallelStateDB GetState access untouched object after create, may check create2")
				return common.Hash{}
			}
			return dirtyObj.GetState(hash)
		}
	}

	if keys, ok := s.parallel.kvChangesInSlot[addr]; ok {
		if _, ok := keys[hash]; ok {
			return dirtyObj.GetState(hash)
		}
	}
	// 2.Try to get from unconfirmed DB or main DB
	// 2.1 Already read before
	if storage, ok := s.parallel.kvReadsInSlot[addr]; ok {
		if val, ok := storage.GetValue(hash); ok {
			return val
		}
	}
	// 2.2 Object in dirty because of other changes, such as getBalance etc.
	// load from dirty directly and the stateObject.GetState() will care of the KvReadInSlot update.
	// So there is no chance for create different objects with same address. (one in dirty and one from non-slot, and inconsistency)
	if dirtyObj != nil {
		return dirtyObj.GetState(hash)
	}

	value := common.Hash{}
	// 2.3 Try to get from unconfirmed DB if exist
	if val, ok := s.getKVFromUnconfirmedDB(addr, hash); ok {
		value = val
	} else {
		// 3.Get from main StateDB
		object := s.getStateObjectNoSlot(addr)
		val = common.Hash{}
		if object != nil {
			val = object.GetState(hash)
		}
		value = val
	}
	if s.parallel.kvReadsInSlot[addr] == nil {
		s.parallel.kvReadsInSlot[addr] = newStorage(false)
	}
	return value
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// So it should not access/update dirty, and not check delete of dirty objects.
func (s *ParallelStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {

	// 2.Try to get from unconfirmed DB or main DB
	//   KVs in unconfirmed DB can be seen as pending storage
	//   KVs in main DB are merged from SlotDB and has done finalise() on merge, can be seen as pending storage too.
	// 2.1 Already read before
	if storage, ok := s.parallel.kvReadsInSlot[addr]; ok {
		if val, ok := storage.GetValue(hash); ok {
			return val
		}
	}
	value := common.Hash{}
	// 2.2 Try to get from unconfirmed DB if exist
	if val, ok := s.getKVFromUnconfirmedDB(addr, hash); ok {
		value = val
	} else {
		// 3. Try to get from main DB
		val = common.Hash{}
		object := s.getStateObjectNoSlot(addr)
		if object != nil {
			val = object.GetCommittedState(hash)
		}
		value = val
	}
	if s.parallel.kvReadsInSlot[addr] == nil {
		s.parallel.kvReadsInSlot[addr] = newStorage(false)
	}
	s.parallel.kvReadsInSlot[addr].StoreValue(hash, value) // update cache

	return value
}

func (s *ParallelStateDB) HasSelfDestructed(addr common.Address) bool {
	// 1.Try to get from dirty
	if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
		if obj == nil || obj.deleted {
			return false
		}
		return obj.selfDestructed
	}
	// 2.Try to get from unconfirmed
	if exist, ok := s.getAddrStateFromUnconfirmedDB(addr, false); ok {
		return !exist
	}

	object := s.getDeletedStateObject(addr)
	if object != nil {
		return object.selfDestructed
	}
	return false
}

// AddBalance adds amount to the account associated with addr.
func (s *ParallelStateDB) AddBalance(addr common.Address, amount *uint256.Int) {
	// add balance will perform a read operation first
	// if amount == 0, no balance change, but there is still an empty check.
	object := s.GetOrNewStateObject(addr)
	if object != nil {
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s) // light copy from main DB
			// do balance fixup from the confirmed DB, it could be more reliable than main DB
			balance := s.GetBalance(addr) // it will record the balance read operation
			newStateObject.setBalance(balance)
			newStateObject.AddBalance(amount)
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			s.parallel.balanceChangesInSlot[addr] = struct{}{}
			return
		}
		// already dirty, make sure the balance is fixed up since it could be previously dirtied by nonce or KV...
		balance := s.GetBalance(addr)
		if object.Balance().Cmp(balance) != 0 {
			log.Warn("AddBalance in dirty, but balance has not do fixup", "txIndex", s.txIndex, "addr", addr,
				"stateObject.Balance()", object.Balance(), "s.GetBalance(addr)", balance)
			object.setBalance(balance)
		}

		object.AddBalance(amount)
		s.parallel.balanceChangesInSlot[addr] = struct{}{}
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *ParallelStateDB) SubBalance(addr common.Address, amount *uint256.Int) {
	// unlike add, sub 0 balance will not touch empty object
	object := s.GetOrNewStateObject(addr)
	if object != nil {
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s) // light copy from main DB
			// do balance fixup from the confirmed DB, it could be more reliable than main DB
			balance := s.GetBalance(addr)
			newStateObject.setBalance(balance)
			newStateObject.SubBalance(amount)
			s.parallel.balanceChangesInSlot[addr] = struct{}{}
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			return
		}
		// already dirty, make sure the balance is fixed up since it could be previously dirtied by nonce or KV...
		balance := s.GetBalance(addr)
		if object.Balance().Cmp(balance) != 0 {
			log.Warn("SubBalance in dirty, but balance is incorrect", "txIndex", s.txIndex, "addr", addr,
				"stateObject.Balance()", object.Balance(), "s.GetBalance(addr)", balance)
			object.setBalance(balance)
		}
		object.SubBalance(amount)
		s.parallel.balanceChangesInSlot[addr] = struct{}{}
	}
}

func (s *ParallelStateDB) SetBalance(addr common.Address, amount *uint256.Int) {
	object := s.GetOrNewStateObject(addr)
	if object != nil {
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s)
			// update balance for revert, in case child contract is reverted,
			// it should revert to the previous balance
			balance := s.GetBalance(addr)
			newStateObject.setBalance(balance)
			newStateObject.SetBalance(amount)
			s.parallel.balanceChangesInSlot[addr] = struct{}{}
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			return
		}

		balance := s.GetBalance(addr)
		object.setBalance(balance)
		object.SetBalance(amount)
		s.parallel.balanceChangesInSlot[addr] = struct{}{}
	}
}

func (s *ParallelStateDB) SetNonce(addr common.Address, nonce uint64) {
	object := s.GetOrNewStateObject(addr)
	if object != nil {
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s)
			noncePre := s.GetNonce(addr)
			newStateObject.setNonce(noncePre) // nonce fixup
			newStateObject.SetNonce(nonce)
			s.parallel.nonceChangesInSlot[addr] = struct{}{}
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			return
		}
		noncePre := s.GetNonce(addr)
		object.setNonce(noncePre) // nonce fixup
		object.SetNonce(nonce)
		s.parallel.nonceChangesInSlot[addr] = struct{}{}
	}
}

func (s *ParallelStateDB) SetCode(addr common.Address, code []byte) {
	object := s.GetOrNewStateObject(addr)
	if object != nil {
		codeHash := crypto.Keccak256Hash(code)
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s)
			codePre := s.GetCode(addr) // code fixup
			codeHashPre := crypto.Keccak256Hash(codePre)
			newStateObject.setCode(codeHashPre, codePre)
			newStateObject.SetCode(codeHash, code)
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			s.parallel.codeChangesInSlot[addr] = struct{}{}
			return
		}
		codePre := s.GetCode(addr) // code fixup
		codeHashPre := crypto.Keccak256Hash(codePre)
		object.setCode(codeHashPre, codePre)
		object.SetCode(codeHash, code)
		s.parallel.codeChangesInSlot[addr] = struct{}{}
	}
}

func (s *ParallelStateDB) SetState(addr common.Address, key, value common.Hash) {
	object := s.GetOrNewStateObject(addr) // attention: if StateObject's lightCopy, its storage is only a part of the full storage,
	if object != nil {
		if s.parallel.baseTxIndex+1 == s.txIndex {
			// we check if state is unchanged
			// only when current transaction is the next transaction to be committed
			// fixme: there is a bug, block: 14,962,284,
			//        stateObject is in dirty (light copy), but the key is in mainStateDB
			//        stateObject dirty -> committed, will skip mainStateDB dirty
			if s.GetState(addr, key) == value {
				log.Debug("Skip set same state", "baseTxIndex", s.parallel.baseTxIndex,
					"txIndex", s.txIndex, "addr", addr,
					"key", key, "value", value)
				return
			}
		}

		if s.parallel.kvChangesInSlot[addr] == nil {
			s.parallel.kvChangesInSlot[addr] = make(StateKeys) // make(Storage, defaultNumOfSlots)
		}

		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			newStateObject := object.lightCopy(s)
			newStateObject.SetState(key, value)
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			s.parallel.addrStateChangesInSlot[addr] = true
			return
		}
		// do State Update
		object.SetState(key, value)
		s.parallel.addrStateChangesInSlot[addr] = true
	}
}

// SelfDestruct marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *ParallelStateDB) SelfDestruct(addr common.Address) {
	var object *stateObject
	// 1.Try to get from dirty, it could be suicided inside of contract call
	object = s.parallel.dirtiedStateObjectsInSlot[addr]

	if object != nil && object.deleted {
		return
	}

	if object == nil {
		// 2.Try to get from unconfirmed, if deleted return false, since the address does not exist
		if obj, ok := s.getStateObjectFromUnconfirmedDB(addr); ok {
			object = obj
			// Treat selfDestructed in unconfirmedDB as deleted since it will be finalised at merge phase.
			deleted := object.deleted || object.selfDestructed
			s.parallel.addrStateReadsInSlot[addr] = !deleted // true: exist, false: deleted
			if deleted {
				return
			}
		}
	}

	if object == nil {
		// 3.Try to get from main StateDB
		object = s.getStateObjectNoSlot(addr)
		if object == nil || object.deleted {
			s.parallel.addrStateReadsInSlot[addr] = false // true: exist, false: deleted
			return
		}
		s.parallel.addrStateReadsInSlot[addr] = true // true: exist, false: deleted
	}

	s.journal.append(selfDestructChange{
		account:     &addr,
		prev:        object.selfDestructed, // todo: must be false?
		prevbalance: new(uint256.Int).Set(s.GetBalance(addr)),
	})

	if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
		// do copy-on-write for suicide "write"
		newStateObject := object.lightCopy(s)
		newStateObject.markSelfdestructed()
		newStateObject.setBalance(new(uint256.Int))
		s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
		s.parallel.addrStateChangesInSlot[addr] = false // false: the address does not exist any more,
		// s.parallel.nonceChangesInSlot[addr] = struct{}{}
		s.parallel.balanceChangesInSlot[addr] = struct{}{}
		s.parallel.codeChangesInSlot[addr] = struct{}{}
		// s.parallel.kvChangesInSlot[addr] = make(StateKeys) // all key changes are discarded
		return
	}

	s.parallel.addrStateChangesInSlot[addr] = false // false: the address does not exist anymore
	s.parallel.balanceChangesInSlot[addr] = struct{}{}
	s.parallel.codeChangesInSlot[addr] = struct{}{}
	object.markSelfdestructed()
	object.setBalance(new(uint256.Int))
}

func (s *ParallelStateDB) Selfdestruct6780(addr common.Address) {
	object := s.getStateObject(addr)
	if object == nil {
		return
	}
	if object.created {
		s.SelfDestruct(addr)
	}
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//  1. sends funds to sha(account ++ (nonce + 1))
//  2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (s *ParallelStateDB) CreateAccount(addr common.Address) {
	// no matter it is got from dirty, unconfirmed or main DB
	// if addr not exist, preBalance will be common.U2560, it is same as new(uint256.Int) which
	// is the value newObject(),
	preBalance := s.GetBalance(addr) // parallel balance read will be recorded inside GetBalance
	newObj := s.createObject(addr)
	newObj.setBalance(new(uint256.Int).Set(preBalance)) // new uint256.Int for newObj
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *ParallelStateDB) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// AddRefund adds gas to the refund counter
// journal.append will use ParallelState for revert
func (s *ParallelStateDB) AddRefund(gas uint64) { // todo: not needed, can be deleted
	s.journal.append(refundChange{prev: s.refund})
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *ParallelStateDB) SubRefund(gas uint64) {
	s.journal.append(refundChange{prev: s.refund})
	if gas > s.refund {
		// we don't need to panic here if we read the wrong state in parallel mode
		// we just need to redo this transaction
		log.Info(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund), "tx", s.thash.String())
		s.parallel.needsRedo = true
		return
	}
	s.refund -= gas
}

// For Parallel Execution Mode, it can be seen as Penetrated Access:
//
//	-------------------------------------------------------
//	| BaseTxIndex | Unconfirmed Txs... | Current TxIndex |
//	-------------------------------------------------------
//
// Access from the unconfirmed DB with range&priority:  txIndex -1(previous tx) -> baseTxIndex + 1
func (s *ParallelStateDB) getBalanceFromUnconfirmedDB(addr common.Address) *uint256.Int {
	for i := s.txIndex - 1; i >= 0 && i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)
		// 1.Refer the state of address, exist or not in dirtiedStateObjectsInSlot
		balanceHit := false
		if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
			balanceHit = true
		}
		if _, exist := db.parallel.balanceChangesInSlot[addr]; exist { // only changed balance is reliable
			balanceHit = true
		}
		if !balanceHit {
			continue
		}
		obj := db.parallel.dirtiedStateObjectsInSlot[addr]
		balance := obj.Balance()
		if obj.deleted {
			balance = common.U2560
		}
		return balance

	}
	return nil
}

// Similar to getBalanceFromUnconfirmedDB
func (s *ParallelStateDB) getNonceFromUnconfirmedDB(addr common.Address) (uint64, bool) {
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)

		nonceHit := false
		if _, ok := db.parallel.addrStateChangesInSlot[addr]; ok {
			nonceHit = true
		} else if _, ok := db.parallel.nonceChangesInSlot[addr]; ok {
			nonceHit = true
		}
		if !nonceHit {
			// nonce refer not hit, try next unconfirmedDb
			continue
		}
		// nonce hit, return the nonce
		obj := db.parallel.dirtiedStateObjectsInSlot[addr]
		if obj == nil {
			// could not exist, if it is changed but reverted
			// fixme: revert should remove the change record
			log.Debug("Get nonce from UnconfirmedDB, changed but object not exist, ",
				"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
			continue
		}
		// deleted object with nonce == 0
		if obj.deleted || obj.selfDestructed {
			return 0, true
		}
		nonce := obj.Nonce()
		return nonce, true
	}
	return 0, false
}

// Similar to getBalanceFromUnconfirmedDB
// It is not only for code, but also codeHash and codeSize, we return the *stateObject for convenience.
func (s *ParallelStateDB) getCodeFromUnconfirmedDB(addr common.Address) ([]byte, bool) {
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)

		codeHit := false
		if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
			codeHit = true
		}
		if _, exist := db.parallel.codeChangesInSlot[addr]; exist {
			codeHit = true
		}
		if !codeHit {
			// try next unconfirmedDb
			continue
		}
		obj := db.parallel.dirtiedStateObjectsInSlot[addr]
		if obj == nil {
			// could not exist, if it is changed but reverted
			// fixme: revert should remove the change record
			log.Debug("Get code from UnconfirmedDB, changed but object not exist, ",
				"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
			continue
		}
		if obj.deleted || obj.selfDestructed {
			return nil, true
		}
		code := obj.Code()
		return code, true
	}
	return nil, false
}

// Similar to getCodeFromUnconfirmedDB
// but differ when address is deleted or not exist
func (s *ParallelStateDB) getCodeHashFromUnconfirmedDB(addr common.Address) (common.Hash, bool) {
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)

		hashHit := false
		if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
			hashHit = true
		}
		if _, exist := db.parallel.codeChangesInSlot[addr]; exist {
			hashHit = true
		}
		if !hashHit {
			// try next unconfirmedDb
			continue
		}
		obj := db.parallel.dirtiedStateObjectsInSlot[addr]
		if obj == nil {
			// could not exist, if it is changed but reverted
			// fixme: revert should remove the change record
			log.Debug("Get codeHash from UnconfirmedDB, changed but object not exist, ",
				"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
			continue
		}
		if obj.deleted || obj.selfDestructed {
			return common.Hash{}, true
		}
		codeHash := common.BytesToHash(obj.CodeHash())
		return codeHash, true
	}
	return common.Hash{}, false
}

// Similar to getCodeFromUnconfirmedDB
// It is for address state check of: Exist(), Empty() and HasSuicided()
// Since the unconfirmed DB should have done Finalise() with `deleteEmptyObjects = true`
// If the dirty address is empty or suicided, it will be marked as deleted, so we only need to return `deleted` or not.
func (s *ParallelStateDB) getAddrStateFromUnconfirmedDB(addr common.Address, testEmpty bool) (bool, bool) {
	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)
		if exist, ok := db.parallel.addrStateChangesInSlot[addr]; ok {
			if obj, ok := db.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				// could not exist, if it is changed but reverted
				// fixme: revert should remove the change record
				log.Debug("Get addr State from UnconfirmedDB, changed but object not exist, ",
					"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
				continue
			} else {
				if obj.selfDestructed || obj.deleted {
					return false, true
				}
				if testEmpty && obj.empty() {
					return false, true
				}
			}
			return exist, true
		}
	}
	return false, false
}

func (s *ParallelStateDB) getKVFromUnconfirmedDB(addr common.Address, key common.Hash) (common.Hash, bool) {
	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)
		if _, ok := db.parallel.kvChangesInSlot[addr]; ok {
			obj := db.parallel.dirtiedStateObjectsInSlot[addr]
			if obj.deleted || obj.selfDestructed {
				return common.Hash{}, true
			}
			if val, exist := obj.dirtyStorage.GetValue(key); exist {
				return val, true
			}
		}
	}
	return common.Hash{}, false
}

func (s *ParallelStateDB) GetStateObjectFromUnconfirmedDB(addr common.Address) (*stateObject, bool) {
	return s.getStateObjectFromUnconfirmedDB(addr)
}

func (s *ParallelStateDB) getStateObjectFromUnconfirmedDB(addr common.Address) (*stateObject, bool) {
	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.BaseTxIndex(); i-- {
		db_, ok := s.parallel.unconfirmedDBs.Load(i)
		if !ok {
			continue
		}
		db := db_.(*ParallelStateDB)
		if obj, ok := db.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			return obj, true
		}
	}
	return nil, false
}

// IsParallelReadsValid If stage2 is true, it is a likely conflict check,
// to detect these potential conflict results in advance and schedule redo ASAP.
func (slotDB *ParallelStateDB) IsParallelReadsValid(isStage2 bool) bool {
	parallelKvOnce.Do(func() {
		StartKvCheckLoop()
	})

	mainDB := slotDB.parallel.baseStateDB
	// for nonce
	for addr, nonceSlot := range slotDB.parallel.nonceReadsInSlot {
		if isStage2 { // update slotDB's unconfirmed DB list and try
			if nonceUnconfirm, ok := slotDB.getNonceFromUnconfirmedDB(addr); ok {
				if nonceSlot != nonceUnconfirm {
					log.Debug("IsSlotDBReadsValid nonce read is invalid in unconfirmed", "addr", addr,
						"nonceSlot", nonceSlot, "nonceUnconfirm", nonceUnconfirm, "SlotIndex", slotDB.parallel.SlotIndex,
						"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
					return false
				}
			}
		}
		nonceMain := mainDB.GetNonce(addr)
		if nonceSlot != nonceMain {
			log.Debug("IsSlotDBReadsValid nonce read is invalid", "addr", addr,
				"nonceSlot", nonceSlot, "nonceMain", nonceMain, "SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// balance
	for addr, balanceSlot := range slotDB.parallel.balanceReadsInSlot {
		if isStage2 { // update slotDB's unconfirmed DB list and try
			if balanceUnconfirm := slotDB.getBalanceFromUnconfirmedDB(addr); balanceUnconfirm != nil {
				if balanceSlot.Cmp(balanceUnconfirm) == 0 {
					continue
				}
				return false
			}
		}

		balanceMain := mainDB.GetBalance(addr)
		if balanceSlot.Cmp(balanceMain) != 0 {
			log.Debug("IsSlotDBReadsValid balance read is invalid", "addr", addr,
				"balanceSlot", balanceSlot, "balanceMain", balanceMain, "SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// check KV
	var units []ParallelKvCheckUnit // todo: pre-allocate to make it faster
	for addr, read := range slotDB.parallel.kvReadsInSlot {
		read.Range(func(keySlot, valSlot interface{}) bool {
			units = append(units, ParallelKvCheckUnit{addr, keySlot.(common.Hash), valSlot.(common.Hash)})
			return true
		})
	}
	readLen := len(units)
	// TODO-dav: change back to 8 or 1?
	if readLen < 80000 || isStage2 {
		for _, unit := range units {
			if hasKvConflict(slotDB, unit.addr, unit.key, unit.val, isStage2) {
				return false
			}
		}
	} else {
		msgHandledNum := 0
		msgSendNum := 0
		for _, unit := range units {
			for { // make sure the unit is consumed
				consumed := false
				select {
				case conflict := <-parallelKvCheckResCh:
					msgHandledNum++
					if conflict {
						// make sure all request are handled or discarded
						for {
							if msgHandledNum == msgSendNum {
								break
							}
							select {
							case <-parallelKvCheckReqCh:
								msgHandledNum++
							case <-parallelKvCheckResCh:
								msgHandledNum++
							}
						}
						return false
					}
				case parallelKvCheckReqCh <- ParallelKvCheckMessage{slotDB, isStage2, unit}:
					msgSendNum++
					consumed = true
				}
				if consumed {
					break
				}
			}
		}
		for {
			if msgHandledNum == readLen {
				break
			}
			conflict := <-parallelKvCheckResCh
			msgHandledNum++
			if conflict {
				// make sure all request are handled or discarded
				for {
					if msgHandledNum == msgSendNum {
						break
					}
					select {
					case <-parallelKvCheckReqCh:
						msgHandledNum++
					case <-parallelKvCheckResCh:
						msgHandledNum++
					}
				}
				return false
			}
		}
	}
	if isStage2 { // stage2 skip check code, or state, since they are likely unchanged.
		return true
	}

	// check code
	for addr, codeSlot := range slotDB.parallel.codeReadsInSlot {
		codeMain := mainDB.GetCode(addr)
		if !bytes.Equal(codeSlot, codeMain) {
			log.Debug("IsSlotDBReadsValid code read is invalid", "addr", addr,
				"len codeSlot", len(codeSlot), "len codeMain", len(codeMain), "SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// check codeHash
	for addr, codeHashSlot := range slotDB.parallel.codeHashReadsInSlot {
		codeHashMain := mainDB.GetCodeHash(addr)
		if !bytes.Equal(codeHashSlot.Bytes(), codeHashMain.Bytes()) {
			log.Debug("IsSlotDBReadsValid codehash read is invalid", "addr", addr,
				"codeHashSlot", codeHashSlot, "codeHashMain", codeHashMain, "SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// addr state check
	for addr, stateSlot := range slotDB.parallel.addrStateReadsInSlot {
		stateMain := false // addr not exist
		if mainDB.getStateObject(addr) != nil {
			stateMain = true // addr exist in main DB
		}
		if stateSlot != stateMain {
			log.Debug("IsSlotDBReadsValid addrState read invalid(true: exist, false: not exist)",
				"addr", addr, "stateSlot", stateSlot, "stateMain", stateMain,
				"SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// snapshot destructs check
	for addr, destructRead := range slotDB.parallel.addrSnapDestructsReadsInSlot {
		mainObj := mainDB.getStateObject(addr)
		if mainObj == nil {
			log.Debug("IsSlotDBReadsValid snapshot destructs read invalid, address should exist",
				"addr", addr, "destruct", destructRead,
				"SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
		slotDB.snapParallelLock.RLock()               // fixme: this lock is not needed
		_, destructMain := mainDB.snapDestructs[addr] // addr not exist
		slotDB.snapParallelLock.RUnlock()
		if destructRead != destructMain {
			log.Debug("IsSlotDBReadsValid snapshot destructs read invalid",
				"addr", addr, "destructRead", destructRead, "destructMain", destructMain,
				"SlotIndex", slotDB.parallel.SlotIndex,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	return true
}

// NeedsRedo returns true if there is any clear reason that we need to redo this transaction
func (s *ParallelStateDB) NeedsRedo() bool {
	return s.parallel.needsRedo
}

// FinaliseForParallel finalises the state by removing the destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
// It also handle the mainDB dirties for the first TX.
func (s *ParallelStateDB) FinaliseForParallel(deleteEmptyObjects bool, mainDB *StateDB) {
	addressesToPrefetch := make([][]byte, 0, len(s.journal.dirties))

	if s.TxIndex() == 0 && len(mainDB.journal.dirties) > 0 {
		for addr := range mainDB.journal.dirties {
			var obj *stateObject
			var exist bool
			obj, exist = mainDB.getStateObjectFromStateObjects(addr)
			if !exist {
				// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
				// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
				// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
				// it will persist in the journal even though the journal is reverted. In this special circumstance,
				// it may exist in `s.journal.dirties` but not in `s.stateObjects`.
				// Thus, we can safely ignore it here
				continue
			}

			if obj.selfDestructed || (deleteEmptyObjects && obj.empty()) {

				obj.deleted = true

				// We need to maintain account deletions explicitly (will remain
				// set indefinitely). Note only the first occurred self-destruct
				// event is tracked.
				if _, ok := mainDB.stateObjectsDestruct[obj.address]; !ok {
					mainDB.stateObjectsDestruct[obj.address] = obj.origin
				}

				// Note, we can't do this only at the end of a block because multiple
				// transactions within the same block might self destruct and then
				// resurrect an account; but the snapshotter needs both events.
				mainDB.AccountMux.Lock()
				delete(mainDB.accounts, obj.addrHash)      // Clear out any previously updated account data (may be recreated via a resurrect)
				delete(mainDB.accountsOrigin, obj.address) // Clear out any previously updated account data (may be recreated via a resurrect)
				mainDB.AccountMux.Unlock()
				
				mainDB.StorageMux.Lock()
				delete(mainDB.storages, obj.addrHash)      // Clear out any previously updated storage data (may be recreated via a resurrect)
				delete(mainDB.storagesOrigin, obj.address) // Clear out any previously updated storage data (may be recreated via a resurrect)
				mainDB.StorageMux.Unlock()
			} else {
				obj.finalise(true) // Prefetch slots in the background
			}

			obj.created = false
			mainDB.stateObjectsPending[addr] = struct{}{}
			mainDB.stateObjectsDirty[addr] = struct{}{}

			// At this point, also ship the address off to the precacher. The precacher
			// will start loading tries, and when the change is eventually committed,
			// the commit-phase will be a lot faster
			addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
		}
		mainDB.clearJournalAndRefund()
	}

	for addr := range s.journal.dirties {
		var obj *stateObject
		var exist bool
		if s.parallel.isSlotDB {
			obj = s.parallel.dirtiedStateObjectsInSlot[addr]
			if obj != nil {
				exist = true
			} else {
				log.Error("StateDB Finalise dirty addr not in dirtiedStateObjectsInSlot",
					"addr", addr)
			}
		} else {
			obj, exist = s.getStateObjectFromStateObjects(addr)
		}
		if !exist {
			// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `s.journal.dirties` but not in `s.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}

		if obj.selfDestructed || (deleteEmptyObjects && obj.empty()) {
			obj.deleted = true
			// We need to maintain account deletions explicitly (will remain
			// set indefinitely). Note only the first occurred self-destruct
			// event is tracked.
			if _, ok := s.stateObjectsDestruct[obj.address]; !ok {
				s.stateObjectsDestruct[obj.address] = obj.origin
			}

			// Note, we can't do this only at the end of a block because multiple
			// transactions within the same block might self destruct and then
			// resurrect an account; but the snapshotter needs both events.
			mainDB.AccountMux.Lock()
			delete(mainDB.accounts, obj.addrHash)      // Clear out any previously updated account data (may be recreated via a resurrect)
			delete(mainDB.accountsOrigin, obj.address) // Clear out any previously updated account data (may be recreated via a resurrect)
			mainDB.AccountMux.Unlock()
			mainDB.StorageMux.Lock()
			delete(mainDB.storages, obj.addrHash)      // Clear out any previously updated storage data (may be recreated via a resurrect)
			delete(mainDB.storagesOrigin, obj.address) // Clear out any previously updated storage data (may be recreated via a resurrect)
			mainDB.StorageMux.Unlock()

			// todo: The following record seems unnecessary.
			if s.parallel.isSlotDB {
				s.parallel.accountsDeletedRecord = append(s.parallel.accountsDeletedRecord, obj.addrHash)
				s.parallel.storagesDeleteRecord = append(s.parallel.storagesDeleteRecord, obj.addrHash)
				s.parallel.accountsOriginDeleteRecord = append(s.parallel.accountsOriginDeleteRecord, obj.address)
				s.parallel.storagesOriginDeleteRecord = append(s.parallel.storagesOriginDeleteRecord, obj.address)
			}

		} else {
			// 1.none parallel mode, we do obj.finalise(true) as normal
			// 2.with parallel mode, we do obj.finalise(true) on dispatcher, not on slot routine
			//   obj.finalise(true) will clear its dirtyStorage, will make prefetch broken.
			if !s.isParallel || !s.parallel.isSlotDB {
				obj.finalise(true) // Prefetch slots in the background
			} else {
				obj.fixUpOriginAndResetPendingStorage()
			}
		}

		obj.created = false
		s.stateObjectsPending[addr] = struct{}{}
		s.stateObjectsDirty[addr] = struct{}{}

		// At this point, also ship the address off to the precacher. The precacher
		// will start loading tries, and when the change is eventually committed,
		// the commit-phase will be a lot faster
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
	}

	if mainDB.prefetcher != nil && len(addressesToPrefetch) > 0 {
		mainDB.prefetcher.prefetch(common.Hash{}, s.originalRoot, common.Address{}, addressesToPrefetch)
	}
	// Invalidate journal because reverting across transactions is not allowed.
	s.clearJournalAndRefund()
}

// IntermediateRootForSlotDB computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
// For parallel SlotDB, the intermediateRoot can be used to calculate the temporary root after executing single tx.
func (s *ParallelStateDB) IntermediateRootForSlotDB(deleteEmptyObjects bool, mainDB *StateDB) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	s.FinaliseForParallel(deleteEmptyObjects, mainDB)

	// If there was a trie prefetcher operating, it gets aborted and irrevocably
	// modified after we start retrieving tries. Remove it from the statedb after
	// this round of use.
	//
	// This is weird pre-byzantium since the first tx runs with a prefetcher and
	// the remainder without, but pre-byzantium even the initial prefetcher is
	// useless, so no sleep lost.
	prefetcher := mainDB.prefetcher
	if mainDB.prefetcher != nil {
		defer func() {
			mainDB.prefetcher.close()
			mainDB.prefetcher = nil
		}()
	}

	if s.TxIndex() == 0 && len(mainDB.stateObjectsPending) > 0 {
		for addr := range mainDB.stateObjectsPending {
			var obj *stateObject
			if obj, _ = mainDB.getStateObjectFromStateObjects(addr); !obj.deleted {
				obj.updateRoot()
			}
		}
	}

	// Although naively it makes sense to retrieve the account trie and then do
	// the contract storage and account updates sequentially, that short circuits
	// the account prefetcher. Instead, let's process all the storage updates
	// first, giving the account prefetches just a few more milliseconds of time
	// to pull useful data from disk.
	for addr := range s.stateObjectsPending {
		var obj *stateObject
		if s.parallel.isSlotDB {
			if obj = s.parallel.dirtiedStateObjectsInSlot[addr]; !obj.deleted {
				obj.updateRoot()
			}
		} else {
			if obj, _ = s.getStateObjectFromStateObjects(addr); !obj.deleted {
				obj.updateRoot()
			}
		}
	}

	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	// The parallel execution do the change incrementally, so can not check the prefetcher here
	if prefetcher != nil {
		if trie := prefetcher.trie(common.Hash{}, mainDB.originalRoot); trie != nil {
			mainDB.trie = trie
		}
	}

	usedAddrs := make([][]byte, 0, len(s.stateObjectsPending))

	if s.TxIndex() == 0 && len(mainDB.stateObjectsPending) > 0 {
		usedAddrs = make([][]byte, 0, len(s.stateObjectsPending)+len(mainDB.stateObjectsPending))
		for addr := range mainDB.stateObjectsPending {
			if obj, _ := mainDB.getStateObjectFromStateObjects(addr); obj.deleted {
				mainDB.deleteStateObject(obj)
				mainDB.AccountDeleted += 1
			} else {
				mainDB.updateStateObject(obj)
				mainDB.AccountUpdated += 1
			}
			usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
		}
	}

	for addr := range s.stateObjectsPending {
		if s.parallel.isSlotDB {
			if obj := s.parallel.dirtiedStateObjectsInSlot[addr]; obj.deleted {
				mainDB.deleteStateObject(obj)
				mainDB.AccountDeleted += 1
			} else {
				mainDB.updateStateObject(obj)
				mainDB.AccountUpdated += 1
			}
		} else if obj, _ := s.getStateObjectFromStateObjects(addr); obj.deleted {
			mainDB.deleteStateObject(obj)
			mainDB.AccountDeleted += 1
		} else {
			mainDB.updateStateObject(obj)
			mainDB.AccountUpdated += 1
		}
		usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
	}

	if prefetcher != nil {
		prefetcher.used(common.Hash{}, mainDB.originalRoot, usedAddrs)
	}
	// parallel slotDB trie will be updated to mainDB since intermediateRoot happens after conflict check.
	// so it should be save to clear pending here.
	// otherwise there can be a case that the deleted object get ignored and processes as live object in verify phase.

	if s.TxIndex() == 0 && len(mainDB.stateObjectsPending) > 0 {
		mainDB.stateObjectsPending = make(map[common.Address]struct{})
	}

	if /*s.isParallel == false &&*/ len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { mainDB.AccountHashes += time.Since(start) }(time.Now())
	}
	ret := mainDB.trie.Hash()

	return ret
}
