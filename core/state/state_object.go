// Copyright 2014 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/opcodeCompiler/compiler"
	"golang.org/x/exp/slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/holiman/uint256"
)

var emptyCodeHash = crypto.Keccak256(nil)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage interface {
	String() string
	GetValue(hash common.Hash) (common.Hash, bool)
	StoreValue(hash common.Hash, value common.Hash)
	Length() (length int)
	Copy() Storage
	Range(func(key, value interface{}) bool)
}

type StorageMap map[common.Hash]common.Hash

func (s StorageMap) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}
	return
}

func (s StorageMap) Copy() Storage {
	cpy := make(StorageMap, len(s))
	for key, value := range s {
		cpy[key] = value
	}

	return cpy
}

func (s StorageMap) GetValue(hash common.Hash) (common.Hash, bool) {
	value, ok := s[hash]
	return value, ok
}

func (s StorageMap) StoreValue(hash common.Hash, value common.Hash) {
	s[hash] = value
}

func (s StorageMap) Length() int {
	return len(s)
}

func (s StorageMap) Range(f func(hash, value interface{}) bool) {
	for k, v := range s {
		result := f(k, v)
		if !result {
			return
		}
	}
}

type StorageSyncMap struct {
	sync.Map
}

func (s *StorageSyncMap) String() (str string) {
	s.Range(func(key, value interface{}) bool {
		str += fmt.Sprintf("%X : %X\n", key, value)
		return true
	})

	return
}

func (s *StorageSyncMap) GetValue(hash common.Hash) (common.Hash, bool) {
	value, ok := s.Load(hash)
	if !ok {
		return common.Hash{}, ok
	}

	return value.(common.Hash), ok
}

func (s *StorageSyncMap) StoreValue(hash common.Hash, value common.Hash) {
	s.Store(hash, value)
}

func (s *StorageSyncMap) Length() (length int) {
	s.Range(func(key, value interface{}) bool {
		length++
		return true
	})
	return length
}

func (s *StorageSyncMap) Copy() Storage {
	cpy := StorageSyncMap{}
	s.Range(func(key, value interface{}) bool {
		cpy.Store(key, value)
		return true
	})

	return &cpy
}

func newStorage(isParallel bool) Storage {
	if isParallel {
		return &StorageSyncMap{}
	}
	return make(StorageMap)
}

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// - First you need to obtain a state object.
// - Account values as well as storages can be accessed and modified through the object.
// - Finally, call commit to return the changes of storage trie and update account data.
//
// NOTICE: For Parallel, there is lightCopy and deepCopy used for cloning object between
// slot DB and global DB, and it is not guaranteed to be happened after finalise(), so any
// field added into the stateObject must be handled in lightCopy and deepCopy.
type stateObject struct {
	db       *StateDB            // The baseDB for parallel.
	dbItf    StateDBer           // The slotDB for parallel.
	address  common.Address      // address of ethereum account
	addrHash common.Hash         // hash of ethereum address of the account
	origin   *types.StateAccount // Account original data without any change applied, nil means it was not existent
	data     types.StateAccount  // Account data with all mutations applied in the scope of block

	// dirty account state
	dirtyBalance  *uint256.Int
	dirtyNonce    *uint64
	dirtyCodeHash []byte

	// Write caches.
	trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	// isParallel indicates this state object is used in parallel mode, in which mode the
	// storage would be sync.Map instead of map
	isParallel bool

	originStorage  Storage // Storage cache of original entries to dedup rewrites
	pendingStorage Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   Storage // Storage entries that have been modified in the current transaction execution, reset for every transaction

	// Cache flags.
	dirtyCode bool // true if the code was updated

	// Flag whether the account was marked as self-destructed. The self-destructed account
	// is still accessible in the scope of same transaction.
	selfDestructed bool

	// Flag whether the account was marked as deleted. A self-destructed account
	// or an account that is considered as empty will be marked as deleted at
	// the end of transaction and no longer accessible anymore.
	deleted bool

	// Flag whether the object was created in the current transaction
	created bool
}

// empty returns whether the account is considered empty.
func (s *stateObject) empty() bool {
	// return s.data.Nongn() == 0 && bytes.ce == 0 && s.data.Balance.SiEqual(s.data.CodeHash, types.EmptyCodeHash.Bytes())
	// return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, emptyCodeHash)

	// empty() has 3 use cases:
	// 1.StateDB.Empty(), to empty check
	//   A: It is ok, we have handled it in Empty(), to make sure nonce, balance, codeHash are solid
	// 2:AddBalance 0, empty check for touch event
	//   empty() will add a touch event.
	//   if we misjudge it, the touch event could be lost, which make address not deleted.  // fixme
	// 3.Finalise(), to do empty delete
	//   the address should be dirtied or touched
	//   if it nonce dirtied, it is ok, since nonce is monotonically increasing, won't be zero
	//   if balance is dirtied, balance could be zero, we should refer solid nonce & codeHash  // fixme
	//   if codeHash is dirtied, it is ok, since code will not be updated.
	//   if suicide, it is ok
	//   if object is new created, it is ok
	//   if CreateAccount, recreate the address, it is ok.

	// Slot 0 tx 0: AddBalance(100) to addr_1, => addr_1: balance = 100, nonce = 0, code is empty
	// Slot 1 tx 1: addr_1 Transfer 99.9979 with GasFee 0.0021, => addr_1: balance = 0, nonce = 1, code is empty
	//              notice: balance transfer cost 21,000 gas, with gasPrice = 100Gwei, GasFee will be 0.0021
	// Slot 0 tx 2: add balance 0 to addr_1(empty check for touch event),
	//              the object was lightCopied from tx 0,

	// in parallel mode, we should not check empty by raw nonce, balance, codeHash anymore,
	// since it could be invalid.
	// e.g., AddBalance() to an address, we will do lightCopy to get a new StateObject, we did balance fixup to
	// make sure object's Balance is reliable. But we did not fixup nonce or code, we only do nonce or codehash
	// fixup on need, that's when we wanna to update the nonce or codehash.
	// So nonce, balance
	// Before the block is processed, addr_1 account: nonce = 0, emptyCodeHash, balance = 100
	//   Slot 0 tx 0: no access to addr_1
	//   Slot 1 tx 1: sub balance 100, it is empty and deleted
	//   Slot 0 tx 2: GetNonce, lightCopy based on main DB(balance = 100) , not empty

	if s.dbItf.GetBalance(s.address).Sign() != 0 { // check balance first, since it is most likely not zero
		return false
	}
	if s.dbItf.GetNonce(s.address) != 0 {
		return false
	}
	codeHash := s.dbItf.GetCodeHash(s.address)
	return bytes.Equal(codeHash.Bytes(), emptyCodeHash) // code is empty, the object is empty

}

// newObject creates a state object.
func newObject(dbItf StateDBer, isParallel bool, address common.Address, acct *types.StateAccount) *stateObject {
	db := dbItf.getBaseStateDB()
	var (
		origin  = acct
		created = acct == nil // true if the account was not existent
	)
	if acct == nil {
		acct = types.NewEmptyStateAccount()
	}
	s := &stateObject{
		db:             db,
		dbItf:          dbItf,
		address:        address,
		addrHash:       crypto.Keccak256Hash(address[:]),
		origin:         origin,
		data:           *acct.Copy(),
		isParallel:     isParallel,
		originStorage:  newStorage(isParallel),
		pendingStorage: newStorage(isParallel),
		dirtyStorage:   newStorage(isParallel),
		created:        created,
	}

	// dirty data when create a new account

	if created {
		s.dirtyBalance = new(uint256.Int).Set(acct.Balance)
		s.dirtyNonce = new(uint64)
		*s.dirtyNonce = acct.Nonce
		s.dirtyCodeHash = acct.CodeHash
	}
	return s
}

// EncodeRLP implements rlp.Encoder.
func (s *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

func (s *stateObject) markSelfdestructed() {
	s.selfDestructed = true
}

func (s *stateObject) touch() {
	s.db.journal.append(touchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

// getTrie returns the associated storage trie. The trie will be opened
// if it's not loaded previously. An error will be returned if trie can't
// be loaded.
func (s *stateObject) getTrie() (Trie, error) {
	if s.trie == nil {
		// Try fetching from prefetcher first
		if s.data.Root != types.EmptyRootHash && s.db.prefetcher != nil {
			// When the miner is creating the pending state, there is no prefetcher
			s.trie = s.db.prefetcher.trie(s.addrHash, s.data.Root)
		}
		if s.trie == nil {
			tr, err := s.db.db.OpenStorageTrie(s.db.originalRoot, s.address, s.data.Root, s.db.trie)
			if err != nil {
				return nil, err
			}
			s.trie = tr
		}
	}
	return s.trie, nil
}

// GetState retrieves a value from the account storage trie.
func (s *stateObject) GetState(key common.Hash) common.Hash {
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage.GetValue(key)
	if dirty {
		return value
	}
	// Otherwise return the entry's original value
	result := s.GetCommittedState(key)
	// Record first read for conflict verify
	if s.db.isParallel && s.db.parallel.isSlotDB {
		addr := s.address
		if s.db.parallel.kvReadsInSlot[addr] == nil {
			s.db.parallel.kvReadsInSlot[addr] = newStorage(false)
		}
		s.db.parallel.kvReadsInSlot[addr].StoreValue(key, result)
	}
	return result
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stateObject) GetCommittedState(key common.Hash) common.Hash {
	// If we have a pending write or clean cached, return that
	// if value, pending := s.pendingStorage[key]; pending {
	if value, pending := s.pendingStorage.GetValue(key); pending {
		return value
	}
	if value, cached := s.originStorage.GetValue(key); cached {
		return value
	}

	if s.db.isParallel && s.db.parallel.isSlotDB {
		// Add-Dav:
		// Need to confirm the object is not destructed in unconfirmed db and resurrected in this tx.
		// otherwise there is an issue for cases like:
		//	B0: TX0 --> createAccount @addr1	-- merged into DB
		//			  B1: Tx1 and Tx2
		//			      Tx1 account@addr1, setState(key0), setState(key1) selfDestruct  -- unconfirmed
		//			      Tx2 recreate account@addr2, setState(key0)  	-- executing
		//			      TX2 GetState(addr2, key1) ---
		//			 key1 is never set after recurrsect, and should not return state in trie as it destructed in unconfirmed
		// TODO - dav: do we need try storages from unconfirmedDB? - currently not because conflict detection need it for get from mainDB.
		obj, exist := s.dbItf.GetStateObjectFromUnconfirmedDB(s.address)
		if exist {
			if obj.deleted || obj.selfDestructed {
				return common.Hash{}
			}
		}

		// also test whether the object is in mainDB and deleted.
		pdb := s.db.parallel.baseStateDB
		obj, exist = pdb.getStateObjectFromStateObjects(s.address)
		if exist {
			if obj.deleted || obj.selfDestructed {
				return common.Hash{}
			}
		}
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	//if _, destructed := s.db.stateObjectsDestruct[s.address]; destructed {
	s.db.snapParallelLock.RLock()
	if _, destructed := s.db.queryStateObjectsDestruct(s.address); destructed { // fixme: use sync.Map, instead of RWMutex?
		s.db.snapParallelLock.RUnlock()
		return common.Hash{}
	}
	s.db.snapParallelLock.RUnlock()

	// If no live objects are available, attempt to use snapshots
	var (
		enc   []byte
		err   error
		value common.Hash
	)
	if s.db.snap != nil {
		start := time.Now()
		enc, err = s.db.snap.Storage(s.addrHash, crypto.Keccak256Hash(key.Bytes()))
		if metrics.EnabledExpensive {
			s.db.SnapshotStorageReads += time.Since(start)
		}
		if len(enc) > 0 {
			_, content, _, err := rlp.Split(enc)
			if err != nil {
				s.db.setError(err)
			}
			value.SetBytes(content)
		}
	}
	// If the snapshot is unavailable or reading from it fails, load from the database.
	if s.db.snap == nil || err != nil {
		start := time.Now()
		tr, err := s.getTrie()
		if err != nil {
			s.db.setError(err)
			return common.Hash{}
		}
		s.db.trieParallelLock.Lock()
		val, err := tr.GetStorage(s.address, key.Bytes())
		s.db.trieParallelLock.Unlock()
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			s.db.setError(err)
			return common.Hash{}
		}
		value.SetBytes(val)
	}
	s.originStorage.StoreValue(key, value)
	return value
}

// SetState updates a value in account storage.
func (s *stateObject) SetState(key, value common.Hash) {
	// If the new value is the same as old, don't set
	// In parallel mode, it has to get from StateDB, in case:
	//  a.the Slot did not set the key before and try to set it to `val_1`
	//  b.Unconfirmed DB has set the key to `val_2`
	//  c.if we use StateObject.GetState, and the key load from the main DB is `val_1`
	//    this `SetState could be skipped`
	//  d.Finally, the key's value will be `val_2`, while it should be `val_1`
	// such as: https://bscscan.com/txs?block=2491181
	prev := s.dbItf.GetState(s.address, key)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	s.db.journal.append(storageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})

	if s.db.isParallel && s.db.parallel.isSlotDB {
		s.db.parallel.kvChangesInSlot[s.address][key] = struct{}{} // should be moved to here, after `s.db.GetState()`
	}
	s.setState(key, value)
}

func (s *stateObject) setState(key, value common.Hash) {
	s.dirtyStorage.StoreValue(key, value)
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stateObject) finalise(prefetch bool) {
	slotsToPrefetch := make([][]byte, 0, s.dirtyStorage.Length())
	s.dirtyStorage.Range(func(key, value interface{}) bool {
		s.pendingStorage.StoreValue(key.(common.Hash), value.(common.Hash))
		originalValue, _ := s.originStorage.GetValue(key.(common.Hash))
		if value.(common.Hash) != originalValue {
			originalKey := key.(common.Hash)
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(originalKey[:])) // Copy needed for closure
		}
		return true
	})
	if s.dirtyNonce != nil {
		s.data.Nonce = *s.dirtyNonce
		s.dirtyNonce = nil
	}
	if s.dirtyBalance != nil {
		s.data.Balance = s.dirtyBalance
		s.dirtyBalance = nil
	}
	if s.dirtyCodeHash != nil {
		s.data.CodeHash = s.dirtyCodeHash
		s.dirtyCodeHash = nil
	}
	if s.db.prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && s.data.Root != types.EmptyRootHash {
		s.db.prefetcher.prefetch(s.addrHash, s.data.Root, s.address, slotsToPrefetch)
	}
	if s.dirtyStorage.Length() > 0 {
		s.dirtyStorage = newStorage(s.isParallel)
	}
}

func (s *stateObject) finaliseRWSet() {
	s.dirtyStorage.Range(func(key, value interface{}) bool {
		// three are some unclean dirtyStorage from previous reverted txs, it will skip finalise
		// so add a new rule, if val has no change, then skip it
		if value == s.GetCommittedState(key.(common.Hash)) {
			return true
		}
		s.db.RecordWrite(types.StorageStateKey(s.address, key.(common.Hash)), value.(common.Hash))
		return true
	})

	if s.dirtyNonce != nil && *s.dirtyNonce != s.data.Nonce {
		s.db.RecordWrite(types.AccountStateKey(s.address, types.AccountNonce), *s.dirtyNonce)
	}
	if s.dirtyBalance != nil && s.dirtyBalance.Cmp(s.data.Balance) != 0 {
		s.db.RecordWrite(types.AccountStateKey(s.address, types.AccountBalance), new(uint256.Int).Set(s.dirtyBalance))
	}
	if s.dirtyCodeHash != nil && !slices.Equal(s.dirtyCodeHash, s.data.CodeHash) {
		s.db.RecordWrite(types.AccountStateKey(s.address, types.AccountCodeHash), s.dirtyCodeHash)
	}
}

// updateTrie is responsible for persisting cached storage changes into the
// object's storage trie. In case the storage trie is not yet loaded, this
// function will load the trie automatically. If any issues arise during the
// loading or updating of the trie, an error will be returned. Furthermore,
// this function will return the mutated storage trie, or nil if there is no
// storage change at all.
func (s *stateObject) updateTrie() (Trie, error) {
	maindb := s.db
	if s.db.isParallel && s.db.parallel.isSlotDB {
		// we need to fixup the origin storage with the mainDB. otherwise the changes maybe problematic since the origin
		// is wrong.
		maindb = s.db.parallel.baseStateDB
		// For dirty/pending/origin Storage access and update.
		maindb.accountStorageParallelLock.Lock()
		defer maindb.accountStorageParallelLock.Unlock()
	}
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise(false)

	// Short circuit if nothing changed, don't bother with hashing anything
	if s.pendingStorage.Length() == 0 {
		return s.trie, nil
	}
	// Track the amount of time wasted on updating the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { maindb.StorageUpdates += time.Since(start) }(time.Now())
	}
	// The snapshot storage map for the object
	var (
		storage map[common.Hash][]byte
		origin  map[common.Hash][]byte
		hasher  = crypto.NewKeccakState()
	)
	tr, err := s.getTrie()
	if err != nil {
		maindb.setError(err)
		return nil, err
	}

	// Insert all the pending storage updates into the trie
	usedStorage := make([][]byte, 0, s.pendingStorage.Length())
	dirtyStorage := make(map[common.Hash][]byte)

	s.pendingStorage.Range(func(keyItf, valueItf interface{}) bool {
		key := keyItf.(common.Hash)
		value := valueItf.(common.Hash)
		// Skip noop changes, persist actual changes
		originalValue, _ := s.originStorage.GetValue(key)
		if value == originalValue {
			return true
		}
		var v []byte
		if value != (common.Hash{}) {
			value := value
			v = common.TrimLeftZeroes(value[:])
		}
		dirtyStorage[key] = v
		return true
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for key, value := range dirtyStorage {
			if len(value) == 0 {
				if err := tr.DeleteStorage(s.address, key[:]); err != nil {
					maindb.setError(err)
				}
				maindb.StorageDeleted += 1
			} else {
				if err := tr.UpdateStorage(s.address, key[:], value); err != nil {
					maindb.setError(err)
				}
				maindb.StorageUpdated += 1
			}
			// Cache the items for preloading
			usedStorage = append(usedStorage, common.CopyBytes(key[:]))
		}
	}()
	// If state snapshotting is active, cache the data til commit
	wg.Add(1)
	go func() {
		defer wg.Done()
		maindb.StorageMux.Lock()
		defer maindb.StorageMux.Unlock()
		// The snapshot storage map for the object
		storage = maindb.storages[s.addrHash]
		if storage == nil {
			storage = make(map[common.Hash][]byte, len(dirtyStorage))
			maindb.storages[s.addrHash] = storage
		}
		// Cache the original value of mutated storage slots
		origin = maindb.storagesOrigin[s.address]
		if origin == nil {
			origin = make(map[common.Hash][]byte)
			maindb.storagesOrigin[s.address] = origin
		}
		for key, value := range dirtyStorage {
			khash := crypto.HashData(hasher, key[:])

			// rlp-encoded value to be used by the snapshot
			var encoded []byte
			if len(value) != 0 {
				encoded, _ = rlp.EncodeToBytes(value)
			}
			storage[khash] = encoded // encoded will be nil if it's deleted

			// Track the original value of slot only if it's mutated first time
			prev, _ := s.originStorage.GetValue(key)
			s.originStorage.StoreValue(key, common.BytesToHash(value)) // fill back left zeroes by BytesToHash
			if _, ok := origin[khash]; !ok {
				if prev == (common.Hash{}) {
					origin[khash] = nil // nil if it was not present previously
				} else {
					// Encoding []byte cannot fail, ok to ignore the error.
					b, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(prev[:]))
					origin[khash] = b
				}
			}
		}
	}()
	wg.Wait()

	if maindb.prefetcher != nil {
		maindb.prefetcher.used(s.addrHash, s.data.Root, usedStorage)
	}

	s.pendingStorage = newStorage(s.isParallel) // reset pending map
	return tr, nil
	/*
		s.pendingStorage.Range(func(keyItf, valueItf interface{}) bool {
			key := keyItf.(common.Hash)
			value := valueItf.(common.Hash)
			// Skip noop changes, persist actual changes
			originalValue, _ := s.originStorage.GetValue(key)
			if value == originalValue {
				return true
			}

			prev, _ := s.originStorage.GetValue(key)
			s.originStorage.StoreValue(key, value)

			var encoded []byte // rlp-encoded value to be used by the snapshot
			if (value == common.Hash{}) {
				if err := tr.DeleteStorage(s.address, key[:]); err != nil {
					maindb.setError(err)
				}
				maindb.StorageDeleted += 1
			} else {
				// Encoding []byte cannot fail, ok to ignore the error.
				trimmed := common.TrimLeftZeroes(value[:])
				encoded, _ = rlp.EncodeToBytes(trimmed)
				if err := tr.UpdateStorage(s.address, key[:], trimmed); err != nil {
					maindb.setError(err)
				}
				maindb.StorageUpdated += 1
			}
			// Cache the mutated storage slots until commit
			if storage == nil {
				if storage = maindb.storages[s.addrHash]; storage == nil {
					storage = make(map[common.Hash][]byte)
					maindb.storages[s.addrHash] = storage
				}
			}

			khash := crypto.HashData(maindb.hasher, key[:])
			storage[khash] = encoded // encoded will be nil if it's deleted

			// Cache the original value of mutated storage slots
			if origin == nil {
				if origin = maindb.storagesOrigin[s.address]; origin == nil {
					origin = make(map[common.Hash][]byte)
					maindb.storagesOrigin[s.address] = origin
				}
			}
			// Track the original value of slot only if it's mutated first time
			if _, ok := origin[khash]; !ok {
				if prev == (common.Hash{}) {
					origin[khash] = nil // nil if it was not present previously
				} else {
					// Encoding []byte cannot fail, ok to ignore the error.
					b, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(prev[:]))
					origin[khash] = b
				}
			}
			// Cache the items for preloading
			usedStorage = append(usedStorage, common.CopyBytes(key[:])) // Copy needed for closure
			return true
		})
		if maindb.prefetcher != nil {
			maindb.prefetcher.used(s.addrHash, s.data.Root, usedStorage)
		}
		s.pendingStorage = newStorage(s.isParallel) // reset pending map

		return tr, nil
	*/
}

// updateRoot flushes all cached storage mutations to trie, recalculating the
// new storage trie root.
func (s *stateObject) updateRoot() {
	// Flush cached storage mutations into trie, short circuit if any error
	// is occurred or there is not change in the trie.
	// TODO: The trieParallelLock seems heavy, can we remove it?
	s.db.trieParallelLock.Lock()
	defer s.db.trieParallelLock.Unlock()

	tr, err := s.updateTrie()
	if err != nil || tr == nil {
		return
	}
	// Track the amount of time wasted on hashing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}
	s.data.Root = tr.Hash()
}

// commit obtains a set of dirty storage trie nodes and updates the account data.
// The returned set can be nil if nothing to commit. This function assumes all
// storage mutations have already been flushed into trie by updateRoot.
func (s *stateObject) commit() (*trienode.NodeSet, error) {
	// Short circuit if trie is not even loaded, don't bother with committing anything
	if s.trie == nil {
		s.origin = s.data.Copy()
		return nil, nil
	}
	// Track the amount of time wasted on committing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}
	// The trie is currently in an open state and could potentially contain
	// cached mutations. Call commit to acquire a set of nodes that have been
	// modified, the set can be nil if nothing to commit.
	root, nodes, err := s.trie.Commit(false)
	if err != nil {
		return nil, err
	}
	s.data.Root = root

	// Update original account data after commit
	s.origin = s.data.Copy()

	return nodes, nil
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *stateObject) AddBalance(amount *uint256.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		if s.empty() {
			s.touch()
		}
		return
	}
	s.SetBalance(new(uint256.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *stateObject) SubBalance(amount *uint256.Int) {
	if amount.IsZero() {
		return
	}
	s.SetBalance(new(uint256.Int).Sub(s.Balance(), amount))
}

func (s *stateObject) SetBalance(amount *uint256.Int) {
	s.db.journal.append(balanceChange{
		account: &s.address,
		prev:    new(uint256.Int).Set(s.Balance()),
	})
	s.setBalance(amount)
}

func (s *stateObject) setBalance(amount *uint256.Int) {
	s.dirtyBalance = amount
}

// ReturnGas Return the gas back to the origin. Used by the Virtual machine or Closures
func (s *stateObject) ReturnGas(gas *uint256.Int) {}

func (s *stateObject) lightCopy(db *ParallelStateDB) *stateObject {
	object := newObject(db, s.isParallel, s.address, &s.data)
	if s.trie != nil {
		s.db.trieParallelLock.Lock()
		object.trie = db.db.CopyTrie(s.trie)
		s.db.trieParallelLock.Unlock()
	}
	object.code = s.code
	object.selfDestructed = s.selfDestructed // should be false
	object.dirtyCode = s.dirtyCode           // it is not used in slot, but keep it is ok
	object.deleted = s.deleted               // should be false

	object.dirtyBalance = s.dirtyBalance
	object.dirtyNonce = s.dirtyNonce
	object.dirtyCodeHash = s.dirtyCodeHash

	// object generated by lightCopy() is supposed to be used in the slot.
	// and the origin storage will be filled at GetState() etc.
	// the dirty and pending will be recorded in the execution for new changes.
	// so no need to do the copy.
	// moreover, copy storage here is tricky, as the stateDB is changed concurrently with
	// the slot execution, and the snap is updated only at Commit stage.
	// so the origin may different between the time NOW and the time of merge, so the conflict check is vital to avoid
	// the problem. fortunately, the KVRead will record this and compare it with mainDB.

	//object.dirtyStorage = s.dirtyStorage.Copy()
	s.db.accountStorageParallelLock.RLock()
	object.originStorage = s.originStorage.Copy()
	object.pendingStorage = s.pendingStorage.Copy()
	s.db.accountStorageParallelLock.RUnlock()
	return object
}

// deepCopy happens only at global serial execution stage.
// E.g. prepareForParallel and merge (copy slotObj to mainDB)
// otherwise the origin/dirty/pending storages may cause incorrect issue.
func (s *stateObject) deepCopy(db *StateDB) *stateObject {
	object := &stateObject{
		db:         db.getBaseStateDB(),
		dbItf:      db,
		address:    s.address,
		addrHash:   s.addrHash,
		origin:     s.origin,
		data:       *s.data.Copy(),
		isParallel: s.isParallel,
	}
	if s.trie != nil {
		s.db.trieParallelLock.Lock()
		object.trie = db.db.CopyTrie(s.trie)
		s.db.trieParallelLock.Unlock()
	}

	object.code = s.code

	// The lock is unnecessary since deepCopy only invoked at global phase. No concurrent racing.
	object.dirtyStorage = s.dirtyStorage.Copy()
	object.originStorage = s.originStorage.Copy()
	object.pendingStorage = s.pendingStorage.Copy()
	object.selfDestructed = s.selfDestructed
	object.dirtyCode = s.dirtyCode
	object.deleted = s.deleted
	object.dirtyBalance = s.dirtyBalance
	object.dirtyNonce = s.dirtyNonce
	object.dirtyCodeHash = s.dirtyCodeHash

	return object
}

func (s *stateObject) MergeSlotObject(db Database, dirtyObjs *stateObject, keys StateKeys) {
	for key := range keys {
		// In parallel mode, always GetState by StateDB, not by StateObject directly,
		// since it the KV could exist in unconfirmed DB.
		// But here, it should be ok, since the KV should be changed and valid in the SlotDB,
		s.setState(key, dirtyObjs.GetState(key))
	}
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *stateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stateObject) Code() []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := s.db.db.ContractCode(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *stateObject) CodeSize() int {
	if s.code != nil {
		return len(s.code)
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return 0
	}
	size, err := s.db.db.ContractCodeSize(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code size %x: %v", s.CodeHash(), err))
	}
	return size
}

func (s *stateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.dbItf.GetCode(s.address)
	s.db.journal.append(codeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *stateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.dirtyCodeHash = codeHash[:]
	s.dirtyCode = true
	compiler.GenOrLoadOptimizedCode(codeHash, s.code)
}

func (s *stateObject) SetNonce(nonce uint64) {
	prevNonce := s.dbItf.GetNonce(s.address)
	s.db.journal.append(nonceChange{
		account: &s.address,
		prev:    prevNonce,
	})
	s.setNonce(nonce)
}

func (s *stateObject) setNonce(nonce uint64) {
	s.dirtyNonce = &nonce
}

func (s *stateObject) CodeHash() []byte {
	if len(s.dirtyCodeHash) > 0 {
		return s.dirtyCodeHash
	}
	return s.data.CodeHash
}

func (s *stateObject) Balance() *uint256.Int {
	if s.dirtyBalance != nil {
		return s.dirtyBalance
	}
	return s.data.Balance
}

func (s *stateObject) Nonce() uint64 {
	if s.dirtyNonce != nil {
		return *s.dirtyNonce
	}
	return s.data.Nonce
}

func (s *stateObject) Root() common.Hash {
	return s.data.Root
}

// fixUpOriginAndResetPendingStorage is used for slot object only, the target is to fix up the origin storage of the
// object with the latest mainDB. And reset the pendingStorage as the execution recorded the changes in dirty and the
// dirties will be merged to pending at finalise. so the current pendingStorage contains obsoleted info mainly from
// lightCopy()
func (s *stateObject) fixUpOriginAndResetPendingStorage() {
	if s.db.isParallel && s.db.parallel.isSlotDB {
		mainDB := s.db.parallel.baseStateDB
		origObj := mainDB.getStateObjectNoUpdate(s.address)
		mainDB.accountStorageParallelLock.RLock()
		if origObj != nil && origObj.originStorage.Length() != 0 {
			s.originStorage = origObj.originStorage.Copy()
		}
		// isParallel is unnecessary since the pendingStorage for slotObject will be used serially from now on.
		if s.pendingStorage.Length() > 0 {
			s.pendingStorage = newStorage(false)
		}
		mainDB.accountStorageParallelLock.RUnlock()
	}
}
