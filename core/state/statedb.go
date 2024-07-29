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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/holiman/uint256"
)

const (
	// storageDeleteLimit denotes the highest permissible memory allocation
	// employed for contract storage deletion.
	storageDeleteLimit = 512 * 1024 * 1024
)

type revision struct {
	id           int
	journalIndex int
}

var emptyAddr = common.Address{}

type StateKeys map[common.Hash]struct{}

type StateObjectSyncMap struct {
	sync.Map
}

func (s *StateObjectSyncMap) LoadStateObject(addr common.Address) (*stateObject, bool) {
	so, ok := s.Load(addr)
	if !ok {
		return nil, ok
	}
	return so.(*stateObject), ok
}

func (s *StateObjectSyncMap) StoreStateObject(addr common.Address, stateObject *stateObject) {
	s.Store(addr, stateObject)
}

// loadStateObj is the entry for loading state object from stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) loadStateObj(addr common.Address) (*stateObject, bool) {

	if s.isParallel {
		s.parallelStateAccessLock.Lock()
		defer s.parallelStateAccessLock.Unlock()
		ret, ok := s.parallel.stateObjects.LoadStateObject(addr)
		return ret, ok
	}

	obj, ok := s.stateObjects[addr]
	return obj, ok
}

// storeStateObj is the entry for storing state object to stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) storeStateObj(addr common.Address, stateObject *stateObject) {
	if s.isParallel {
		// When a state object is stored into s.parallel.stateObjects,
		// it belongs to base StateDB, it is confirmed and valid.
		// TODO-dav: remove the lock/unlock?
		s.parallelStateAccessLock.Lock()
		s.parallel.stateObjects.StoreStateObject(addr, stateObject)
		s.parallelStateAccessLock.Unlock()
	} else {
		s.stateObjects[addr] = stateObject
	}
}

// deleteStateObj is the entry for deleting state object to stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) deleteStateObj(addr common.Address) {
	if s.isParallel {
		s.parallel.stateObjects.Delete(addr)
	} else {
		delete(s.stateObjects, addr)
	}
}

// ParallelState is for parallel mode only
type ParallelState struct {
	isSlotDB  bool // denotes StateDB is used in slot, we will try to remove it
	SlotIndex int  // for debug, to be removed
	// stateObjects holds the state objects in the base slot db
	// the reason for using stateObjects instead of stateObjects on the outside is
	// we need a thread safe map to hold state objects since there are many slots will read
	// state objects from it;
	// And we will merge all the changes made by the concurrent slot into it.
	stateObjects *StateObjectSyncMap

	baseStateDB               *StateDB // for parallel mode, there will be a base StateDB in dispatcher routine.
	baseTxIndex               int      // slotDB is created base on this tx index.
	dirtiedStateObjectsInSlot map[common.Address]*stateObject
	unconfirmedDBs            *sync.Map /*map[int]*ParallelStateDB*/ // do unconfirmed reference in same slot.

	// we will record the read detail for conflict check and
	// the changed addr or key for object merge, the changed detail can be achieved from the dirty object
	nonceChangesInSlot   map[common.Address]struct{}
	nonceReadsInSlot     map[common.Address]uint64
	balanceChangesInSlot map[common.Address]struct{}     // the address's balance has been changed
	balanceReadsInSlot   map[common.Address]*uint256.Int // the address's balance has been read and used.
	// codeSize can be derived based on code, but codeHash can not be directly derived based on code
	// - codeSize is 0 for address not exist or empty code
	// - codeHash is `common.Hash{}` for address not exist, emptyCodeHash(`Keccak256Hash(nil)`) for empty code,
	// so we use codeReadsInSlot & codeHashReadsInSlot to keep code and codeHash, codeSize is derived from code
	codeReadsInSlot     map[common.Address][]byte // empty if address not exist or no code in this address
	codeHashReadsInSlot map[common.Address]common.Hash
	codeChangesInSlot   map[common.Address]struct{}
	kvReadsInSlot       map[common.Address]Storage
	kvChangesInSlot     map[common.Address]StateKeys // value will be kept in dirtiedStateObjectsInSlot
	// Actions such as SetCode, Suicide will change address's state.
	// Later call like Exist(), Empty(), HasSuicided() depend on the address's state.
	addrStateReadsInSlot   map[common.Address]bool // true: exist, false: not exist or deleted
	addrStateChangesInSlot map[common.Address]bool // true: created, false: deleted

	addrSnapDestructsReadsInSlot map[common.Address]bool

	accountsDeletedRecord      []common.Hash
	storagesDeleteRecord       []common.Hash
	accountsOriginDeleteRecord []common.Address
	storagesOriginDeleteRecord []common.Address

	createdObjectRecord map[common.Address]struct{}

	// Transaction will pay gas fee to system address.
	// Parallel execution will clear system address's balance at first, in order to maintain transaction's
	// gas fee value. Normal transaction will access system address twice, otherwise it means the transaction
	// needs real system address's balance, the transaction will be marked redo with keepSystemAddressBalance = true
	// systemAddress            common.Address
	// systemAddressOpsCount    int
	// keepSystemAddressBalance bool

	// we may need to redo for some specific reasons, like we read the wrong state and need to panic in sequential mode in SubRefund
	needsRedo bool
}

// StateDB structs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
//
// * Contracts
// * Accounts
//
// Once the state is committed, tries cached in stateDB (including account
// trie, storage tries) will no longer be functional. A new state instance
// must be created with new root and updated database for accessing post-
// commit states.
type StateDB struct {
	db         Database
	prefetcher *triePrefetcher
	trie       Trie
	noTrie     bool
	hasher     crypto.KeccakState
	snaps      *snapshot.Tree    // Nil if snapshot is not available
	snap       snapshot.Snapshot // Nil if snapshot is not available

	parallelStateAccessLock sync.RWMutex
	snapParallelLock        sync.RWMutex // for parallel mode, for main StateDB, slot will read snapshot, while processor will write.
	trieParallelLock        sync.Mutex   // for parallel mode of trie, mostly for get states/objects from trie, lock required to handle trie tracer.
	// TODO: is it possible to remove this accountStorageParallelLock?
	accountStorageParallelLock sync.RWMutex // for global state account/storage read (copyForSlot) and write (Intermediate)
	snapDestructs              map[common.Address]struct{}
	snapAccounts               map[common.Address][]byte
	snapStorage                map[common.Address]map[string][]byte

	// originalRoot is the pre-state root, before any changes were made.
	// It will be updated when the Commit is called.
	originalRoot common.Hash
	expectedRoot common.Hash // The state root in the block header
	stateRoot    common.Hash // The calculation result of IntermediateRoot

	fullProcessed bool

	// These maps hold the state changes (including the corresponding
	// original value) that occurred in this **block**.
	AccountMux     sync.Mutex                                // Mutex for accounts access
	StorageMux     sync.Mutex                                // Mutex for storages access
	accounts       map[common.Hash][]byte                    // The mutated accounts in 'slim RLP' encoding
	storages       map[common.Hash]map[common.Hash][]byte    // The mutated slots in prefix-zero trimmed rlp format
	accountsOrigin map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	storagesOrigin map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp format

	// This map holds 'live' objects, which will get modified while processing
	// a state transition.
	stateObjects              map[common.Address]*stateObject
	stateObjectsPending       map[common.Address]struct{}            // State objects finalized but not yet written to the trie
	stateObjectsDirty         map[common.Address]struct{}            // State objects modified in the current execution
	stateObjectsDestruct      map[common.Address]*types.StateAccount // State objects destructed in the block along with its previous value
	stateObjectsDestructDirty map[common.Address]*types.StateAccount

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be
	// returned by StateDB.Commit. Notably, this error is also shared
	// by all cached state objects in case the database failure occurs
	// when accessing state of accounts.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	// The tx context and all occurred logs in the scope of transaction.
	thash   common.Hash
	txIndex int
	logs    map[common.Hash][]*types.Log
	logSize uint

	// parallel EVM related
	rwSet    *types.RWSet
	mvStates *types.MVStates
	es       *types.ExeStat

	// Preimages occurred seen by VM in the scope of block.
	preimages map[common.Hash][]byte

	// Per-transaction access list
	accessList *accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionId int

	// Measurements gathered during execution for debugging purposes
	AccountReads         time.Duration
	AccountHashes        time.Duration
	AccountUpdates       time.Duration
	AccountCommits       time.Duration
	StorageReads         time.Duration
	StorageHashes        time.Duration
	StorageUpdates       time.Duration
	StorageCommits       time.Duration
	SnapshotAccountReads time.Duration
	SnapshotStorageReads time.Duration
	SnapshotCommits      time.Duration
	TrieDBCommits        time.Duration
	TrieCommits          time.Duration
	CodeCommits          time.Duration
	TxDAGGenerate        time.Duration

	AccountUpdated int
	StorageUpdated int
	AccountDeleted int
	StorageDeleted int

	isParallel bool
	parallel   ParallelState // to keep all the parallel execution elements
	// Testing hooks
	onCommit func(states *triestate.Set) // Hook invoked when commit is performed
}

func (s *StateDB) GetStateObjectFromUnconfirmedDB(addr common.Address) (*stateObject, bool) {
	return nil, false
}

// New creates a new state from a given trie.
func New(root common.Hash, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	tr, err := db.OpenTrie(root)

	if err != nil {
		return nil, err
	}
	sdb := &StateDB{
		db:                        db,
		trie:                      tr,
		originalRoot:              root,
		snaps:                     snaps,
		accounts:                  make(map[common.Hash][]byte),
		storages:                  make(map[common.Hash]map[common.Hash][]byte),
		accountsOrigin:            make(map[common.Address][]byte),
		storagesOrigin:            make(map[common.Address]map[common.Hash][]byte),
		stateObjects:              make(map[common.Address]*stateObject),
		stateObjectsPending:       make(map[common.Address]struct{}),
		stateObjectsDirty:         make(map[common.Address]struct{}),
		stateObjectsDestruct:      make(map[common.Address]*types.StateAccount),
		stateObjectsDestructDirty: make(map[common.Address]*types.StateAccount, defaultNumOfSlots),
		logs:                      make(map[common.Hash][]*types.Log),
		preimages:                 make(map[common.Hash][]byte),
		journal:                   newJournal(),
		accessList:                newAccessList(),
		transientStorage:          newTransientStorage(),
		hasher:                    crypto.NewKeccakState(),

		parallel: ParallelState{
			SlotIndex: -1,
		},
		txIndex: -1,
	}
	if sdb.snaps != nil {
		sdb.snap = sdb.snaps.Snapshot(root)
	}
	_, sdb.noTrie = tr.(*trie.EmptyTrie)
	return sdb, nil
}

// NewStateDBByTrie creates a new state db by a given trie.
func NewStateDBByTrie(tr Trie, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	sdb := &StateDB{
		db:                   db,
		trie:                 tr,
		originalRoot:         tr.Hash(),
		snaps:                snaps,
		accounts:             make(map[common.Hash][]byte),
		storages:             make(map[common.Hash]map[common.Hash][]byte),
		accountsOrigin:       make(map[common.Address][]byte),
		storagesOrigin:       make(map[common.Address]map[common.Hash][]byte),
		stateObjects:         make(map[common.Address]*stateObject),
		stateObjectsPending:  make(map[common.Address]struct{}),
		stateObjectsDirty:    make(map[common.Address]struct{}),
		stateObjectsDestruct: make(map[common.Address]*types.StateAccount),
		logs:                 make(map[common.Hash][]*types.Log),
		preimages:            make(map[common.Hash][]byte),
		journal:              newJournal(),
		accessList:           newAccessList(),
		transientStorage:     newTransientStorage(),
		hasher:               crypto.NewKeccakState(),
	}
	if sdb.snaps != nil {
		sdb.snap = sdb.snaps.Snapshot(tr.Hash())
	}
	_, sdb.noTrie = tr.(*trie.EmptyTrie)
	return sdb, nil
}

func (s *StateDB) IsParallel() bool {
	return s.isParallel
}

func (s *StateDB) getBaseStateDB() *StateDB {
	return s
}

func (s *StateDB) getStateObjectFromStateObjects(addr common.Address) (*stateObject, bool) {
	return s.loadStateObj(addr)
}

// StartPrefetcher initializes a new trie prefetcher to pull in nodes from the
// state trie concurrently while the state is mutated so that when we reach the
// commit phase, most of the needed data is already hot.
func (s *StateDB) StartPrefetcher(namespace string) {
	if s.noTrie {
		return
	}

	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
	if s.snap != nil {
		s.prefetcher = newTriePrefetcher(s.db, s.originalRoot, namespace)
	}
}

// StopPrefetcher terminates a running prefetcher and reports any leftover stats
// from the gathered metrics.
func (s *StateDB) StopPrefetcher() {
	if s.noTrie {
		return
	}

	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
}

// Mark that the block is full processed
func (s *StateDB) MarkFullProcessed() {
	s.fullProcessed = true
}

// setError remembers the first non-nil error it is called with.
func (s *StateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// Error returns the memorized database failure occurred earlier.
func (s *StateDB) Error() error {
	return s.dbErr
}

func (s *StateDB) AddLog(log *types.Log) {
	s.journal.append(addLogChange{txhash: s.thash})
	log.TxHash = s.thash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	s.logs[s.thash] = append(s.logs[s.thash], log)
	s.logSize++
}

// GetLogs returns the logs matching the specified transaction hash, and annotates
// them with the given blockNumber and blockHash.
func (s *StateDB) GetLogs(hash common.Hash, blockNumber uint64, blockHash common.Hash) []*types.Log {
	logs := s.logs[hash]
	for _, l := range logs {
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return logs
}

func (s *StateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *StateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := s.preimages[hash]; !ok {
		s.journal.append(addPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		s.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *StateDB) Preimages() map[common.Hash][]byte {
	return s.preimages
}

// AddRefund adds gas to the refund counter
func (s *StateDB) AddRefund(gas uint64) {
	s.journal.append(refundChange{prev: s.refund})
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *StateDB) SubRefund(gas uint64) {
	s.journal.append(refundChange{prev: s.refund})
	if gas > s.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for self-destructed accounts.
func (s *StateDB) Exist(addr common.Address) bool {
	return s.getStateObject(addr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *StateDB) Empty(addr common.Address) bool {
	so := s.getStateObject(addr)
	return so == nil || so.empty()
}

// GetBalance retrieves the balance from the given address or 0 if object not found
func (s *StateDB) GetBalance(addr common.Address) (ret *uint256.Int) {
	defer func() {
		s.RecordRead(types.AccountStateKey(addr, types.AccountBalance), ret)
	}()

	object := s.getStateObject(addr)

	if object != nil {
		return object.Balance()
	}
	return common.U2560
}

// GetNonce retrieves the nonce from the given address or 0 if object not found
func (s *StateDB) GetNonce(addr common.Address) (ret uint64) {
	defer func() {
		s.RecordRead(types.AccountStateKey(addr, types.AccountNonce), ret)
	}()
	object := s.getStateObject(addr)
	if object != nil {
		return object.Nonce()
	}
	return 0
}

// GetStorageRoot retrieves the storage root from the given address or empty
// if object not found.
func (s *StateDB) GetStorageRoot(addr common.Address) common.Hash {
	object := s.getStateObject(addr)
	if object != nil {
		return object.Root()
	}
	return common.Hash{}
}

// TxIndex returns the current transaction index set by Prepare.
func (s *StateDB) TxIndex() int {
	return s.txIndex
}

// BaseTxIndex returns the tx index that slot db based.
func (s *StateDB) BaseTxIndex() int {
	return s.parallel.baseTxIndex
}

func (s *StateDB) GetCode(addr common.Address) []byte {
	defer func() {
		s.RecordRead(types.AccountStateKey(addr, types.AccountCodeHash), s.GetCodeHash(addr))
	}()
	object := s.getStateObject(addr)
	if object != nil {
		return object.Code()
	}
	return nil
}

func (s *StateDB) GetCodeSize(addr common.Address) int {
	defer func() {
		s.RecordRead(types.AccountStateKey(addr, types.AccountCodeHash), s.GetCodeHash(addr))
	}()
	object := s.getStateObject(addr)
	if object != nil {
		return object.CodeSize()
	}
	return 0
}

// GetCodeHash return:
//   - common.Hash{}: the address does not exist
//   - emptyCodeHash: the address exist, but code is empty
//   - others:        the address exist, and code is not empty
func (s *StateDB) GetCodeHash(addr common.Address) (ret common.Hash) {
	defer func() {
		s.RecordRead(types.AccountStateKey(addr, types.AccountCodeHash), ret.Bytes())
	}()
	object := s.getStateObject(addr)
	if object == nil {
		return common.Hash{}
	}
	return common.Hash{}
}

// GetState retrieves a value from the given account's storage trie.
func (s *StateDB) GetState(addr common.Address, hash common.Hash) (ret common.Hash) {
	defer func() {
		s.RecordRead(types.StorageStateKey(addr, hash), ret)
	}()
	object := s.getStateObject(addr)
	if object != nil {
		return object.GetState(hash)
	}
	return common.Hash{}
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *StateDB) GetCommittedState(addr common.Address, hash common.Hash) (ret common.Hash) {
	defer func() {
		s.RecordRead(types.StorageStateKey(addr, hash), ret)
	}()
	object := s.getStateObject(addr)
	if object != nil {
		return object.GetCommittedState(hash)
	}
	return common.Hash{}
}

// Database retrieves the low level database supporting the lower level trie ops.
func (s *StateDB) Database() Database {
	return s.db
}

func (s *StateDB) HasSelfDestructed(addr common.Address) bool {
	object := s.getStateObject(addr)
	if object != nil {
		return object.selfDestructed
	}
	return false
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
func (s *StateDB) AddBalance(addr common.Address, amount *uint256.Int) {
	object := s.getOrNewStateObject(addr)
	if object != nil {
		s.RecordRead(types.AccountStateKey(addr, types.AccountBalance), object.Balance())
		object.AddBalance(amount)
		return
	}
	s.RecordRead(types.AccountStateKey(addr, types.AccountBalance), common.U2560)
}

// SubBalance subtracts amount from the account associated with addr.
func (s *StateDB) SubBalance(addr common.Address, amount *uint256.Int) {
	object := s.getOrNewStateObject(addr)
	if object != nil {
		s.RecordRead(types.AccountStateKey(addr, types.AccountBalance), object.Balance())
		object.SubBalance(amount)
		return
	}
	s.RecordRead(types.AccountStateKey(addr, types.AccountBalance), common.U2560)
}

func (s *StateDB) SetBalance(addr common.Address, amount *uint256.Int) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (s *StateDB) SetNonce(addr common.Address, nonce uint64) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (s *StateDB) SetCode(addr common.Address, code []byte) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *StateDB) SetState(addr common.Address, key, value common.Hash) {

	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging and the mutations
// must be discarded afterwards.
func (s *StateDB) SetStorage(addr common.Address, storage map[common.Hash]common.Hash) {
	// SetStorage needs to wipe existing storage. We achieve this by pretending
	// that the account self-destructed earlier in this block, by flagging
	// it in stateObjectsDestruct. The effect of doing so is that storage lookups
	// will not hit disk, since it is assumed that the disk-data is belonging
	// to a previous incarnation of the object.
	//
	// TODO(rjl493456442) this function should only be supported by 'unwritable'
	// state and all mutations made should all be discarded afterwards.
	if _, ok := s.queryStateObjectsDestruct(addr); !ok {
		s.tagStateObjectsDestruct(addr, nil)
	}
	stateObject := s.getOrNewStateObject(addr)
	for k, v := range storage {
		stateObject.SetState(k, v)
	}
}

// SelfDestruct marks the given account as selfdestructed.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after SelfDestruct.
func (s *StateDB) SelfDestruct(addr common.Address) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return
	}
	s.journal.append(selfDestructChange{
		account:     &addr,
		prev:        stateObject.selfDestructed,
		prevbalance: new(uint256.Int).Set(stateObject.Balance()),
	})
	stateObject.markSelfdestructed()
	stateObject.data.Balance = new(uint256.Int)
}

func (s *StateDB) Selfdestruct6780(addr common.Address) {
	object := s.getStateObject(addr)
	if object == nil {
		return
	}
	if object.created {
		s.SelfDestruct(addr)
	}
}

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (s *StateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	prev := s.GetTransientState(addr, key)
	if prev == value {
		return
	}
	s.journal.append(transientStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})
	s.setTransientState(addr, key, value)
}

// setTransientState is a lower level setter for transient storage. It
// is called during a revert to prevent modifications to the journal.
func (s *StateDB) setTransientState(addr common.Address, key, value common.Hash) {
	s.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (s *StateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	return s.transientStorage.Get(addr, key)
}

//
// Setting, updating & deleting state object methods.
//

// updateStateObject writes the given object to the trie.
func (s *StateDB) updateStateObject(obj *stateObject) {
	if !(s.isParallel && s.parallel.isSlotDB) {
		s.accountStorageParallelLock.Lock()
	}
	if !s.noTrie {
		// Track the amount of time wasted on updating the account from the trie
		if metrics.EnabledExpensive {
			defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
		}
		// Encode the account and update the account trie
		addr := obj.Address()
		s.trieParallelLock.Lock()
		if err := s.trie.UpdateAccount(addr, &obj.data); err != nil {
			s.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
		}
		if obj.dirtyCode {
			s.trie.UpdateContractCode(obj.Address(), common.BytesToHash(obj.CodeHash()), obj.code)
		}
		s.trieParallelLock.Unlock()
	}

	s.AccountMux.Lock()
	defer s.AccountMux.Unlock()
	// Cache the data until commit. Note, this update mechanism is not symmetric
	// to the deletion, because whereas it is enough to track account updates
	// at commit time, deletions need tracking at transaction boundary level to
	// ensure we capture state clearing.
	s.accounts[obj.addrHash] = types.SlimAccountRLP(obj.data)

	// Track the original value of mutated account, nil means it was not present.
	// Skip if it has been tracked (because updateStateObject may be called
	// multiple times in a block).
	if _, ok := s.accountsOrigin[obj.address]; !ok {
		if obj.origin == nil {
			s.accountsOrigin[obj.address] = nil
		} else {
			s.accountsOrigin[obj.address] = types.SlimAccountRLP(*obj.origin)
		}
	}

	if !(s.isParallel && s.parallel.isSlotDB) {
		s.accountStorageParallelLock.Unlock()
	}
}

// deleteStateObject removes the given object from the state trie.
func (s *StateDB) deleteStateObject(obj *stateObject) {
	if s.noTrie {
		return
	}

	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()

	if err := s.trie.DeleteAccount(addr); err != nil {
		s.setError(fmt.Errorf("deleteStateObject (%x) error: %v", addr[:], err))
	}
}

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
func (s *StateDB) getStateObject(addr common.Address) *stateObject {
	obj := s.getDeletedStateObject(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getStateObjectNoUpdate is similar with getStateObject except that it does not
// update stateObjects records.
func (s *StateDB) getStateObjectNoUpdate(addr common.Address) *stateObject {
	obj := s.getDeletedStateObjectNoUpdate(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

func (s *StateDB) getDeletedStateObjectNoUpdate(addr common.Address) *stateObject {
	s.RecordRead(types.AccountStateKey(addr, types.AccountSelf), struct{}{})

	// Prefer live objects if any is available
	if obj, _ := s.getStateObjectFromStateObjects(addr); obj != nil {
		return obj
	}

	data, ok := s.getStateObjectFromSnapshotOrTrie(addr)
	if !ok {
		return nil
	}
	// Insert into the live set
	obj := newObject(s, s.isParallel, addr, data)
	return obj
}

func (s *StateDB) GetStateObjectFromSnapshotOrTrie(addr common.Address) (data *types.StateAccount, ok bool) {
	return s.getStateObjectFromSnapshotOrTrie(addr)
}

func (s *StateDB) SnapHasAccount(addr common.Address) (exist bool) {
	if s.snap == nil {
		return false
	}
	acc, _ := s.snap.Account(crypto.HashData(s.hasher, addr.Bytes()))
	return acc != nil
}

func (s *StateDB) TriHasAccount(addr common.Address) (exist bool) {
	if s.trie == nil {
		return false
	}

	acc, _ := s.trie.GetAccount(addr)
	return acc != nil
}

func (s *StateDB) GetTrie() Trie {
	if s.trie == nil {
		return nil
	}
	return s.trie
}

func (s *StateDB) SetTrie(trie Trie) {
	s.trie = trie
}

func (s *StateDB) getStateObjectFromSnapshotOrTrie(addr common.Address) (data *types.StateAccount, ok bool) {
	// If no live objects are available, attempt to use snapshots
	if s.snap != nil {
		start := time.Now()
		acc, err := s.snap.Account(crypto.HashData(s.hasher, addr.Bytes()))
		if metrics.EnabledExpensive {
			s.SnapshotAccountReads += time.Since(start)
		}
		if err == nil {
			if acc == nil {
				return nil, false
			}

			data = &types.StateAccount{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				CodeHash: acc.CodeHash,
				Root:     common.BytesToHash(acc.Root),
			}
			if len(data.CodeHash) == 0 {
				data.CodeHash = emptyCodeHash
			}
			if data.Root == (common.Hash{}) {
				data.Root = types.EmptyRootHash
			}
		}
	}

	// If snapshot unavailable or reading from it failed, load from the database
	if data == nil {
		s.trieParallelLock.Lock()
		defer s.trieParallelLock.Unlock()
		var trie Trie
		if s.isParallel {
			// hold lock for parallel
			if s.parallel.isSlotDB {
				if s.parallel.baseStateDB == nil {
					return nil, false
				} else {
					tr, err := s.parallel.baseStateDB.db.OpenTrie(s.originalRoot)
					if err != nil {
						log.Error("Can not openTrie for parallel SlotDB\n")
						return nil, false
					}
					trie = tr
				}
			} else {
				trie = s.trie
			}
		} else {
			trie = s.trie
		}

		start := time.Now()
		var err error
		data, err = trie.GetAccount(addr)
		if metrics.EnabledExpensive {
			s.AccountReads += time.Since(start)
		}
		if err != nil {
			s.setError(fmt.Errorf("getDeleteStateObject (%x) error: %w", addr.Bytes(), err))
			return nil, false
		}
		if data == nil {
			return nil, false
		}
	}

	return data, true
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *StateDB) getDeletedStateObject(addr common.Address) *stateObject {
	s.RecordRead(types.AccountStateKey(addr, types.AccountSelf), struct{}{})

	// Prefer live objects if any is available
	if obj, _ := s.getStateObjectFromStateObjects(addr); obj != nil {
		return obj
	}

	data, ok := s.getStateObjectFromSnapshotOrTrie(addr)
	if !ok {
		return nil
	}
	// Insert into the live set
	obj := newObject(s, s.isParallel, addr, data)
	s.storeStateObj(addr, obj)
	return obj
}

func (s *StateDB) setStateObject(object *stateObject) {
	if s.isParallel {
		// When a state object is stored into s.parallel.stateObjects,
		// it belongs to base StateDB, it is confirmed and valid.
		s.parallelStateAccessLock.Lock()
		s.parallel.stateObjects.Store(object.address, object)
		s.parallelStateAccessLock.Unlock()
	} else {
		s.stateObjects[object.Address()] = object
	}
}

// getOrNewStateObject retrieves a state object or create a new state object if nil.
func (s *StateDB) getOrNewStateObject(addr common.Address) *stateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject = s.createObject(addr)
	}
	return stateObject
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
func (s *StateDB) createObject(addr common.Address) (newobj *stateObject) {
	prev := s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!
	newobj = newObject(s, s.isParallel, addr, nil)
	if prev == nil {
		s.journal.append(createObjectChange{account: &addr})
	} else {
		// The original account should be marked as destructed and all cached
		// account and storage data should be cleared as well. Note, it must
		// be done here, otherwise the destruction event of "original account"
		// will be lost.
		_, prevdestruct := s.queryStateObjectsDestruct(prev.address)
		if !prevdestruct {
			s.tagStateObjectsDestruct(prev.address, prev.origin)
		}
		// There may be some cached account/storage data already since IntermediateRoot
		// will be called for each transaction before byzantium fork which will always
		// cache the latest account/storage data.
		prevAccount, ok := s.accountsOrigin[prev.address]
		s.journal.append(resetObjectChange{
			account:                &addr,
			prev:                   prev,
			prevdestruct:           prevdestruct,
			prevAccount:            s.accounts[prev.addrHash],
			prevStorage:            s.storages[prev.addrHash],
			prevAccountOriginExist: ok,
			prevAccountOrigin:      prevAccount,
			prevStorageOrigin:      s.storagesOrigin[prev.address],
		})
		s.AccountMux.Lock()
		delete(s.accounts, prev.addrHash)
		delete(s.accountsOrigin, prev.address)
		s.AccountMux.Unlock()
		s.StorageMux.Lock()
		delete(s.storages, prev.addrHash)
		delete(s.storagesOrigin, prev.address)
		s.StorageMux.Unlock()
	}

	newobj.created = true
	s.setStateObject(newobj)
	return newobj
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
func (s *StateDB) CreateAccount(addr common.Address) {
	// no matter it is got from dirty, unconfirmed or main DB
	// if addr not exist, preBalance will be common.U2560, it is same as new(big.Int) which
	// is the value newObject(),
	preBalance := s.GetBalance(addr)
	newObj := s.createObject(addr)
	newObj.setBalance(new(uint256.Int).Set(preBalance)) // new big.Int for newObj
}

// CopyWithMvStates will copy state with MVStates
func (s *StateDB) CopyWithMvStates(doPrefetch bool) *StateDB {
	state := s.copyInternal(doPrefetch)
	if s.mvStates != nil {
		state.mvStates = s.mvStates
	}
	return state
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *StateDB) Copy() *StateDB {
	return s.copyInternal(false)
}

// CopyDoPrefetch It is mainly for state prefetcher to do trie prefetch right now.
func (s *StateDB) CopyDoPrefetch() *StateDB {
	return s.copyInternal(true)
}

func (s *StateDB) copyInternal(doPrefetch bool) *StateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
		db:                        s.db,
		trie:                      s.db.CopyTrie(s.trie),
		originalRoot:              s.originalRoot,
		accounts:                  make(map[common.Hash][]byte),
		storages:                  make(map[common.Hash]map[common.Hash][]byte),
		accountsOrigin:            make(map[common.Address][]byte),
		storagesOrigin:            make(map[common.Address]map[common.Hash][]byte),
		stateObjects:              make(map[common.Address]*stateObject, len(s.journal.dirties)),
		stateObjectsPending:       make(map[common.Address]struct{}, len(s.stateObjectsPending)),
		stateObjectsDirty:         make(map[common.Address]struct{}, len(s.journal.dirties)),
		stateObjectsDestruct:      make(map[common.Address]*types.StateAccount, len(s.stateObjectsDestruct)),
		stateObjectsDestructDirty: make(map[common.Address]*types.StateAccount, len(s.stateObjectsDestructDirty)),
		refund:                    s.refund,
		logs:                      make(map[common.Hash][]*types.Log, len(s.logs)),
		logSize:                   s.logSize,
		preimages:                 make(map[common.Hash][]byte, len(s.preimages)),
		journal:                   newJournal(),
		hasher:                    crypto.NewKeccakState(),

		// In order for the block producer to be able to use and make additions
		// to the snapshot tree, we need to copy that as well. Otherwise, any
		// block mined by ourselves will cause gaps in the tree, and force the
		// miner to operate trie-backed only.
		snaps: s.snaps,
		snap:  s.snap,

		parallel: ParallelState{},
	}
	// Copy the dirty states, logs, and preimages
	for addr := range s.journal.dirties {
		// As documented [here](https://github.com/ethereum/go-ethereum/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := s.getStateObjectFromStateObjects(addr); exist {
			// Even though the original object is dirty, we are not copying the journal,
			// so we need to make sure that any side-effect the journal would have caused
			// during a commit (or similar op) is already applied to the copy.
			state.storeStateObj(addr, object.deepCopy(state))

			state.stateObjectsDirty[addr] = struct{}{}   // Mark the copy dirty to force internal (code/state) commits
			state.stateObjectsPending[addr] = struct{}{} // Mark the copy pending to force external (account) commits
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy
	// is copied, the loop above will be a no-op, since the copy's journal
	// is empty. Thus, here we iterate over stateObjects, to enable copies
	// of copies.
	for addr := range s.stateObjectsPending {
		if _, exist := state.getStateObjectFromStateObjects(addr); !exist {
			object, _ := s.getStateObjectFromStateObjects(addr)
			state.storeStateObj(addr, object.deepCopy(state))
		}
		state.stateObjectsPending[addr] = struct{}{}
	}
	for addr := range s.stateObjectsDirty {
		if _, exist := state.getStateObjectFromStateObjects(addr); !exist {
			object, _ := s.getStateObjectFromStateObjects(addr)
			state.storeStateObj(addr, object.deepCopy(state))
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}
	// Deep copy the destruction markers.
	for addr, value := range s.stateObjectsDestruct {
		state.stateObjectsDestruct[addr] = value
	}
	for addr, value := range s.stateObjectsDestructDirty {
		state.stateObjectsDestructDirty[addr] = value
	}
	// Deep copy the state changes made in the scope of block
	// along with their original values.
	s.AccountMux.Lock()
	state.accounts = copySet(s.accounts)
	state.accountsOrigin = copySet(state.accountsOrigin)
	s.AccountMux.Unlock()

	s.StorageMux.Lock()
	state.storages = copy2DSet(s.storages)
	state.storagesOrigin = copy2DSet(state.storagesOrigin)
	s.StorageMux.Unlock()

	// Deep copy the logs occurred in the scope of block
	for hash, logs := range s.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}
	// Deep copy the preimages occurred in the scope of block
	for hash, preimage := range s.preimages {
		state.preimages[hash] = preimage
	}
	// Do we need to copy the access list and transient storage?
	// In practice: No. At the start of a transaction, these two lists are empty.
	// In practice, we only ever copy state _between_ transactions/blocks, never
	// in the middle of a transaction. However, it doesn't cost us much to copy
	// empty lists, so we do it anyway to not blow up if we ever decide copy them
	// in the middle of a transaction.
	state.accessList = s.accessList.Copy()
	state.transientStorage = s.transientStorage.Copy()

	// If there's a prefetcher running, make an inactive copy of it that can
	// only access data but does not actively preload (since the user will not
	// know that they need to explicitly terminate an active copy).
	if s.prefetcher != nil {
		state.prefetcher = s.prefetcher.copy()
	}

	return state
}

var journalPool = sync.Pool{
	New: func() interface{} {
		return &journal{
			dirties: make(map[common.Address]int, defaultNumOfSlots),
			entries: make([]journalEntry, 0, defaultNumOfSlots),
		}
	},
}

var addressToStructPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]struct{}, defaultNumOfSlots) },
}

var addressToStateKeysPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]StateKeys, defaultNumOfSlots) },
}

var addressToStoragePool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]Storage, defaultNumOfSlots) },
}

var addressToStateObjectsPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]*stateObject, defaultNumOfSlots) },
}

var balancePool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]*uint256.Int, defaultNumOfSlots) },
}

var addressToHashPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]common.Hash, defaultNumOfSlots) },
}

var addressToBytesPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address][]byte, defaultNumOfSlots) },
}

var addressToBoolPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]bool, defaultNumOfSlots) },
}

var addressToUintPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]uint64, defaultNumOfSlots) },
}

var snapStoragePool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]map[string][]byte, defaultNumOfSlots) },
}

var snapStorageValuePool = sync.Pool{
	New: func() interface{} { return make(map[string][]byte, defaultNumOfSlots) },
}

var logsPool = sync.Pool{
	New: func() interface{} { return make(map[common.Hash][]*types.Log, defaultNumOfSlots) },
}

func (s *StateDB) PutSyncPool() {
	for key := range s.parallel.codeReadsInSlot {
		delete(s.parallel.codeReadsInSlot, key)
	}
	addressToBytesPool.Put(s.parallel.codeReadsInSlot)

	for key := range s.parallel.codeHashReadsInSlot {
		delete(s.parallel.codeHashReadsInSlot, key)
	}
	addressToHashPool.Put(s.parallel.codeHashReadsInSlot)

	for key := range s.parallel.codeChangesInSlot {
		delete(s.parallel.codeChangesInSlot, key)
	}
	addressToStructPool.Put(s.parallel.codeChangesInSlot)

	for key := range s.parallel.kvChangesInSlot {
		delete(s.parallel.kvChangesInSlot, key)
	}
	addressToStateKeysPool.Put(s.parallel.kvChangesInSlot)

	for key := range s.parallel.kvReadsInSlot {
		delete(s.parallel.kvReadsInSlot, key)
	}
	addressToStoragePool.Put(s.parallel.kvReadsInSlot)

	for key := range s.parallel.balanceChangesInSlot {
		delete(s.parallel.balanceChangesInSlot, key)
	}
	addressToStructPool.Put(s.parallel.balanceChangesInSlot)

	for key := range s.parallel.balanceReadsInSlot {
		delete(s.parallel.balanceReadsInSlot, key)
	}
	balancePool.Put(s.parallel.balanceReadsInSlot)

	for key := range s.parallel.addrStateReadsInSlot {
		delete(s.parallel.addrStateReadsInSlot, key)
	}
	addressToBoolPool.Put(s.parallel.addrStateReadsInSlot)

	for key := range s.parallel.addrStateChangesInSlot {
		delete(s.parallel.addrStateChangesInSlot, key)
	}
	addressToBoolPool.Put(s.parallel.addrStateChangesInSlot)

	for key := range s.parallel.nonceChangesInSlot {
		delete(s.parallel.nonceChangesInSlot, key)
	}
	addressToStructPool.Put(s.parallel.nonceChangesInSlot)

	for key := range s.parallel.nonceReadsInSlot {
		delete(s.parallel.nonceReadsInSlot, key)
	}
	addressToUintPool.Put(s.parallel.nonceReadsInSlot)

	for key := range s.parallel.addrSnapDestructsReadsInSlot {
		delete(s.parallel.addrSnapDestructsReadsInSlot, key)
	}
	addressToBoolPool.Put(s.parallel.addrSnapDestructsReadsInSlot)

	for key := range s.parallel.dirtiedStateObjectsInSlot {
		delete(s.parallel.dirtiedStateObjectsInSlot, key)
	}
	addressToStateObjectsPool.Put(s.parallel.dirtiedStateObjectsInSlot)

	for key := range s.stateObjectsPending {
		delete(s.stateObjectsPending, key)
	}
	addressToStructPool.Put(s.stateObjectsPending)

	for key := range s.stateObjectsDirty {
		delete(s.stateObjectsDirty, key)
	}
	addressToStructPool.Put(s.stateObjectsDirty)

	for key := range s.logs {
		delete(s.logs, key)
	}
	logsPool.Put(s.logs)

	for key := range s.journal.dirties {
		delete(s.journal.dirties, key)
	}
	s.journal.entries = s.journal.entries[:0]
	journalPool.Put(s.journal)

	for key := range s.snapDestructs {
		delete(s.snapDestructs, key)
	}
	addressToStructPool.Put(s.snapDestructs)

	for key := range s.parallel.createdObjectRecord {
		delete(s.parallel.createdObjectRecord, key)
	}
	addressToStructPool.Put(s.parallel.createdObjectRecord)

	for key := range s.snapAccounts {
		delete(s.snapAccounts, key)
	}
	addressToBytesPool.Put(s.snapAccounts)

	for key, storage := range s.snapStorage {
		for key := range storage {
			delete(storage, key)
		}
		snapStorageValuePool.Put(storage)
		delete(s.snapStorage, key)
	}
	snapStoragePool.Put(s.snapStorage)
}

// CopyForSlot copy all the basic fields, initialize the memory ones
func (s *StateDB) CopyForSlot() *ParallelStateDB {
	parallel := ParallelState{
		// The stateObjects in Parallel is thread-local.
		// The base stateDB's stateObjects is thread-unsafe as it is not guarded by lock.
		// The base stateDB's parallel.stateObjects is SyncMap and thread-safe. and no extra lock needed (TODO-dav).
		// The base stateDB's parallel.stateObjects are updated by mergeSlotDB with Lock.
		// The base stateDB's stateObject is read-only and never be updated once parallel execution happens.
		// AND, presumably, the stateDB's stateObject is usually empty for real on-chain cases.
		// Before execution, the slotDB should copy objects from base stateDB's parallel.stateObjects and stateObjects
		// NOTICE:
		// We are not reusing the base slot db's stateObjects although copy can be avoid. Because multiple thread
		// access has lock check and there might be tricky bug such as thread1 handle tx0 at the same time with thread2
		// handle tx1, so what thread1's slotDB see in the s.parallel.stateObjects might be the middle result of Thread2.
		//
		// We are not do simple copy (lightweight pointer copy) as the stateObject can be accessed by different thread.
		// Todo-dav: remove lock guard of parallel.stateObject access.

		stateObjects:                 &StateObjectSyncMap{}, // s.parallel.stateObjects,
		codeReadsInSlot:              addressToBytesPool.Get().(map[common.Address][]byte),
		codeHashReadsInSlot:          addressToHashPool.Get().(map[common.Address]common.Hash),
		codeChangesInSlot:            addressToStructPool.Get().(map[common.Address]struct{}),
		kvChangesInSlot:              addressToStateKeysPool.Get().(map[common.Address]StateKeys),
		kvReadsInSlot:                addressToStoragePool.Get().(map[common.Address]Storage),
		balanceChangesInSlot:         addressToStructPool.Get().(map[common.Address]struct{}),
		balanceReadsInSlot:           balancePool.Get().(map[common.Address]*uint256.Int),
		addrStateReadsInSlot:         addressToBoolPool.Get().(map[common.Address]bool),
		addrStateChangesInSlot:       addressToBoolPool.Get().(map[common.Address]bool),
		nonceChangesInSlot:           addressToStructPool.Get().(map[common.Address]struct{}),
		nonceReadsInSlot:             addressToUintPool.Get().(map[common.Address]uint64),
		addrSnapDestructsReadsInSlot: addressToBoolPool.Get().(map[common.Address]bool),
		isSlotDB:                     true,
		dirtiedStateObjectsInSlot:    addressToStateObjectsPool.Get().(map[common.Address]*stateObject),
		accountsDeletedRecord:        make([]common.Hash, 10),
		storagesDeleteRecord:         make([]common.Hash, 10),
		accountsOriginDeleteRecord:   make([]common.Address, 10),
		storagesOriginDeleteRecord:   make([]common.Address, 10),
		createdObjectRecord:          addressToStructPool.Get().(map[common.Address]struct{}),
	}
	state := &ParallelStateDB{
		StateDB: StateDB{
			db:                   s.db,
			trie:                 nil, // Parallel StateDB may access the trie, but it takes no effect to the baseDB.
			accounts:             make(map[common.Hash][]byte),
			storages:             make(map[common.Hash]map[common.Hash][]byte),
			accountsOrigin:       make(map[common.Address][]byte),
			storagesOrigin:       make(map[common.Address]map[common.Hash][]byte),
			stateObjects:         make(map[common.Address]*stateObject), // replaced by parallel.stateObjects in parallel mode
			stateObjectsPending:  addressToStructPool.Get().(map[common.Address]struct{}),
			stateObjectsDirty:    addressToStructPool.Get().(map[common.Address]struct{}),
			stateObjectsDestruct: make(map[common.Address]*types.StateAccount),
			refund:               0, // should be 0
			logs:                 logsPool.Get().(map[common.Hash][]*types.Log),
			logSize:              0,
			preimages:            make(map[common.Hash][]byte, len(s.preimages)),
			journal:              journalPool.Get().(*journal),
			hasher:               crypto.NewKeccakState(),
			isParallel:           true,
			parallel:             parallel,
		},
	}
	// no need to copy preimages, comment out and remove later
	// for hash, preimage := range s.preimages {
	//	state.preimages[hash] = preimage
	// }

	// copy parallel stateObjects
	s.parallelStateAccessLock.Lock()
	s.parallel.stateObjects.Range(func(addr any, stateObj any) bool {
		state.parallel.stateObjects.StoreStateObject(addr.(common.Address), stateObj.(*stateObject).lightCopy(state))
		return true
	})
	s.parallelStateAccessLock.Unlock()
	if s.snaps != nil {
		// In order for the miner to be able to use and make additions
		// to the snapshot tree, we need to copy that as well.
		// Otherwise, any block mined by ourselves will cause gaps in the tree,
		// and force the miner to operate trie-backed only
		state.snaps = s.snaps
		state.snap = s.snap
		// deep copy needed
		state.snapDestructs = addressToStructPool.Get().(map[common.Address]struct{})
		s.snapParallelLock.RLock()
		for k, v := range s.snapDestructs {
			state.snapDestructs[k] = v
		}
		s.snapParallelLock.RUnlock()
		// snapAccounts is useless in SlotDB, comment out and remove later
		// state.snapAccounts = make(map[common.Address][]byte) // snapAccountPool.Get().(map[common.Address][]byte)
		// for k, v := range s.snapAccounts {
		//	state.snapAccounts[k] = v
		// }

		// snapStorage is useless in SlotDB either, it is updated on updateTrie, which is validation phase to update the snapshot of a finalized block.
		// state.snapStorage = snapStoragePool.Get().(map[common.Address]map[string][]byte)
		// for k, v := range s.snapStorage {
		//	temp := snapStorageValuePool.Get().(map[string][]byte)
		//	for kk, vv := range v {
		//		temp[kk] = vv
		//	}
		//	state.snapStorage[k] = temp
		// }

		// trie prefetch should be done by dispatcher on StateObject Merge,
		// disable it in parallel slot
		// state.prefetcher = s.prefetcher
	}

	// Deep copy the state changes made in the scope of block
	// along with their original values.
	s.AccountMux.Lock()
	state.accounts = copySet(s.accounts)
	state.accountsOrigin = copySet(state.accountsOrigin)
	s.AccountMux.Unlock()
	s.StorageMux.Lock()
	state.storages = copy2DSet(s.storages)
	state.storagesOrigin = copy2DSet(state.storagesOrigin)
	s.StorageMux.Unlock()

	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (s *StateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *StateDB) RevertToSnapshot(revid int) {
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

// GetRefund returns the current value of the refund counter.
func (s *StateDB) GetRefund() uint64 {
	return s.refund
}

// Finalise finalises the state by removing the destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
func (s *StateDB) Finalise(deleteEmptyObjects bool) {
	addressesToPrefetch := make([][]byte, 0, len(s.journal.dirties))

	// finalise stateObjectsDestruct
	for addr, acc := range s.stateObjectsDestructDirty {
		s.stateObjectsDestruct[addr] = acc
	}
	s.stateObjectsDestructDirty = make(map[common.Address]*types.StateAccount)
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
			delete(s.accounts, obj.addrHash)      // Clear out any previously updated account data (may be recreated via a resurrect)
			delete(s.storages, obj.addrHash)      // Clear out any previously updated storage data (may be recreated via a resurrect)
			delete(s.accountsOrigin, obj.address) // Clear out any previously updated account data (may be recreated via a resurrect)
			delete(s.storagesOrigin, obj.address) // Clear out any previously updated storage data (may be recreated via a resurrect)

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
	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(common.Hash{}, s.originalRoot, common.Address{}, addressesToPrefetch)
	}
	// Invalidate journal because reverting across transactions is not allowed.
	s.clearJournalAndRefund()
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
// TODO: For parallel SlotDB, IntermediateRootForSlot is used, need to clean up this method.
func (s *StateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	s.Finalise(deleteEmptyObjects)
	s.AccountsIntermediateRoot()
	return s.StateIntermediateRoot()
}

func (s *StateDB) AccountsIntermediateRoot() {
	tasks := make(chan func())
	finishCh := make(chan struct{})
	defer close(finishCh)
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case task := <-tasks:
					task()
				case <-finishCh:
					return
				}
			}
		}()
	}

	// Although naively it makes sense to retrieve the account trie and then do
	// the contract storage and account updates sequentially, that short circuits
	// the account prefetcher. Instead, let's process all the storage updates
	// first, giving the account prefetches just a few more milliseconds of time
	// to pull useful data from disk.
	for addr := range s.stateObjectsPending {
		if s.parallel.isSlotDB {
			if obj := s.parallel.dirtiedStateObjectsInSlot[addr]; !obj.deleted {
				wg.Add(1)
				tasks <- func() {
					defer wg.Done()
					obj.updateRoot()

					// Cache the data until commit. Note, this update mechanism is not symmetric
					// to the deletion, because whereas it is enough to track account updates
					// at commit time, deletions need tracking at transaction boundary level to
					// ensure we capture state clearing.
					s.AccountMux.Lock()
					s.accounts[obj.addrHash] = types.SlimAccountRLP(obj.data)
					s.AccountMux.Unlock()
				}
			}
		} else {
			if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
				wg.Add(1)
				tasks <- func() {
					defer wg.Done()
					obj.updateRoot()

					// Cache the data until commit. Note, this update mechanism is not symmetric
					// to the deletion, because whereas it is enough to track account updates
					// at commit time, deletions need tracking at transaction boundary level to
					// ensure we capture state clearing.
					s.AccountMux.Lock()
					s.accounts[obj.addrHash] = types.SlimAccountRLP(obj.data)
					s.AccountMux.Unlock()
				}
			}
		}
	}
	wg.Wait()
}

func (s *StateDB) StateIntermediateRoot() common.Hash {
	// If there was a trie prefetcher operating, it gets aborted and irrevocably
	// modified after we start retrieving tries. Remove it from the statedb after
	// this round of use.
	//
	// This is weird pre-byzantium since the first tx runs with a prefetcher and
	// the remainder without, but pre-byzantium even the initial prefetcher is
	// useless, so no sleep lost.
	prefetcher := s.prefetcher
	r := s.originalRoot
	if s.prefetcher != nil {
		defer func() {
			s.prefetcher.close()
			s.prefetcher = nil
		}()
		if s.isParallel {
			r = s.trie.Hash()
		}
	}
	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	// The parallel execution do the change incrementally, so can not check the prefetcher here
	if prefetcher != nil {
		if trie := prefetcher.trie(common.Hash{}, r); trie != nil {
			s.trie = trie
		}
	}
	if s.trie == nil {
		tr, err := s.db.OpenTrie(s.originalRoot)
		if err != nil {
			panic(fmt.Sprintf("failed to open trie tree %s", s.originalRoot))
		}
		s.trie = tr
	}

	usedAddrs := make([][]byte, 0, len(s.stateObjectsPending))

	for addr := range s.stateObjectsPending {
		if s.parallel.isSlotDB {
			if obj := s.parallel.dirtiedStateObjectsInSlot[addr]; obj.deleted {
				s.deleteStateObject(obj)
				s.AccountDeleted += 1
			} else {
				s.updateStateObject(obj)
				s.AccountUpdated += 1
			}
		} else if obj, _ := s.getStateObjectFromStateObjects(addr); obj.deleted {
			s.deleteStateObject(obj)
			s.AccountDeleted += 1
		} else {
			s.updateStateObject(obj)
			s.AccountUpdated += 1
		}
		usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
	}
	if prefetcher != nil {
		prefetcher.used(common.Hash{}, s.originalRoot, usedAddrs)
	}
	// parallel slotDB trie will be updated to mainDB since intermediateRoot happens after conflict check.
	// so it should be save to clear pending here.
	// otherwise there can be a case that the deleted object get ignored and processes as live object in verify phase.

	if /*s.isParallel == false &&*/ len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountHashes += time.Since(start) }(time.Now())
	}

	if s.noTrie {
		return s.expectedRoot
	} else {
		return s.trie.Hash()
	}
}

// Mark that the block is processed by diff layer
func (s *StateDB) SetExpectedStateRoot(root common.Hash) {
	s.expectedRoot = root
}

// SetTxContext sets the current transaction hash and index which are
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (s *StateDB) SetTxContext(thash common.Hash, ti int) {
	s.thash = thash
	s.txIndex = ti
}

func (s *StateDB) clearJournalAndRefund() {
	if len(s.journal.entries) > 0 {
		s.journal = newJournal()
		s.refund = 0
	}
	s.validRevisions = s.validRevisions[:0] // Snapshots can be created without journal entries
}

// fastDeleteStorage is the function that efficiently deletes the storage trie
// of a specific account. It leverages the associated state snapshot for fast
// storage iteration and constructs trie node deletion markers by creating
// stack trie with iterated slots.
func (s *StateDB) fastDeleteStorage(addrHash common.Hash, root common.Hash) (bool, common.StorageSize, map[common.Hash][]byte, *trienode.NodeSet, error) {
	iter, err := s.snaps.StorageIterator(s.originalRoot, addrHash, common.Hash{})
	if err != nil {
		return false, 0, nil, nil, err
	}
	defer iter.Release()

	var (
		size  common.StorageSize
		nodes = trienode.NewNodeSet(addrHash)
		slots = make(map[common.Hash][]byte)
	)
	stack := trie.NewStackTrie(func(path []byte, hash common.Hash, blob []byte) {
		nodes.AddNode(path, trienode.NewDeleted())
		size += common.StorageSize(len(path))
	})
	for iter.Next() {
		if size > storageDeleteLimit {
			return true, size, nil, nil, nil
		}
		slot := common.CopyBytes(iter.Slot())
		if err := iter.Error(); err != nil { // error might occur after Slot function
			return false, 0, nil, nil, err
		}
		size += common.StorageSize(common.HashLength + len(slot))
		slots[iter.Hash()] = slot

		if err := stack.Update(iter.Hash().Bytes(), slot); err != nil {
			return false, 0, nil, nil, err
		}
	}
	if err := iter.Error(); err != nil { // error might occur during iteration
		return false, 0, nil, nil, err
	}
	if stack.Hash() != root {
		return false, 0, nil, nil, fmt.Errorf("snapshot is not matched, exp %x, got %x", root, stack.Hash())
	}
	return false, size, slots, nodes, nil
}

// slowDeleteStorage serves as a less-efficient alternative to "fastDeleteStorage,"
// employed when the associated state snapshot is not available. It iterates the
// storage slots along with all internal trie nodes via trie directly.
func (s *StateDB) slowDeleteStorage(addr common.Address, addrHash common.Hash, root common.Hash) (bool, common.StorageSize, map[common.Hash][]byte, *trienode.NodeSet, error) {
	tr, err := s.db.OpenStorageTrie(s.originalRoot, addr, root, s.trie)
	if err != nil {
		return false, 0, nil, nil, fmt.Errorf("failed to open storage trie, err: %w", err)
	}
	it, err := tr.NodeIterator(nil)
	if err != nil {
		return false, 0, nil, nil, fmt.Errorf("failed to open storage iterator, err: %w", err)
	}
	var (
		size  common.StorageSize
		nodes = trienode.NewNodeSet(addrHash)
		slots = make(map[common.Hash][]byte)
	)
	for it.Next(true) {
		if size > storageDeleteLimit {
			return true, size, nil, nil, nil
		}
		if it.Leaf() {
			slots[common.BytesToHash(it.LeafKey())] = common.CopyBytes(it.LeafBlob())
			size += common.StorageSize(common.HashLength + len(it.LeafBlob()))
			continue
		}
		if it.Hash() == (common.Hash{}) {
			continue
		}
		size += common.StorageSize(len(it.Path()))
		nodes.AddNode(it.Path(), trienode.NewDeleted())
	}
	if err := it.Error(); err != nil {
		return false, 0, nil, nil, err
	}
	return false, size, slots, nodes, nil
}

// deleteStorage is designed to delete the storage trie of a designated account.
// It could potentially be terminated if the storage size is excessively large,
// potentially leading to an out-of-memory panic. The function will make an attempt
// to utilize an efficient strategy if the associated state snapshot is reachable;
// otherwise, it will resort to a less-efficient approach.
func (s *StateDB) deleteStorage(addr common.Address, addrHash common.Hash, root common.Hash) (bool, map[common.Hash][]byte, *trienode.NodeSet, error) {
	var (
		start   = time.Now()
		err     error
		aborted bool
		size    common.StorageSize
		slots   map[common.Hash][]byte
		nodes   *trienode.NodeSet
	)
	// The fast approach can be failed if the snapshot is not fully
	// generated, or it's internally corrupted. Fallback to the slow
	// one just in case.
	if s.snap != nil {
		aborted, size, slots, nodes, err = s.fastDeleteStorage(addrHash, root)
	}
	if s.snap == nil || err != nil {
		aborted, size, slots, nodes, err = s.slowDeleteStorage(addr, addrHash, root)
	}
	if err != nil {
		return false, nil, nil, err
	}
	if metrics.EnabledExpensive {
		if aborted {
			slotDeletionSkip.Inc(1)
		}
		n := int64(len(slots))

		slotDeletionMaxCount.UpdateIfGt(int64(len(slots)))
		slotDeletionMaxSize.UpdateIfGt(int64(size))

		slotDeletionTimer.UpdateSince(start)
		slotDeletionCount.Mark(n)
		slotDeletionSize.Mark(int64(size))
	}
	return aborted, slots, nodes, nil
}

// handleDestruction processes all destruction markers and deletes the account
// and associated storage slots if necessary. There are four possible situations
// here:
//
//   - the account was not existent and be marked as destructed
//
//   - the account was not existent and be marked as destructed,
//     however, it's resurrected later in the same block.
//
//   - the account was existent and be marked as destructed
//
//   - the account was existent and be marked as destructed,
//     however it's resurrected later in the same block.
//
// In case (a), nothing needs be deleted, nil to nil transition can be ignored.
//
// In case (b), nothing needs be deleted, nil is used as the original value for
// newly created account and storages
//
// In case (c), **original** account along with its storages should be deleted,
// with their values be tracked as original value.
//
// In case (d), **original** account along with its storages should be deleted,
// with their values be tracked as original value.
func (s *StateDB) handleDestruction(nodes *trienode.MergedNodeSet) (map[common.Address]struct{}, error) {
	// Short circuit if geth is running with hash mode. This procedure can consume
	// considerable time and storage deletion isn't supported in hash mode, thus
	// preemptively avoiding unnecessary expenses.
	incomplete := make(map[common.Address]struct{})
	if s.db.TrieDB().Scheme() == rawdb.HashScheme {
		return incomplete, nil
	}

	for addr, prev := range s.stateObjectsDestruct {
		// The original account was non-existing, and it's marked as destructed
		// in the scope of block. It can be case (a) or (b).
		// - for (a), skip it without doing anything.
		// - for (b), track account's original value as nil. It may overwrite
		//   the data cached in s.accountsOrigin set by 'updateStateObject'.
		addrHash := crypto.Keccak256Hash(addr[:])
		if prev == nil {
			if _, ok := s.accounts[addrHash]; ok {
				s.accountsOrigin[addr] = nil // case (b)
			}
			continue
		}
		// It can overwrite the data in s.accountsOrigin set by 'updateStateObject'.
		s.accountsOrigin[addr] = types.SlimAccountRLP(*prev) // case (c) or (d)

		// Short circuit if the storage was empty.
		if prev.Root == types.EmptyRootHash {
			continue
		}

		// Remove storage slots belong to the account.
		aborted, slots, set, err := s.deleteStorage(addr, addrHash, prev.Root)
		if err != nil {
			return nil, fmt.Errorf("failed to delete storage, err: %w", err)
		}
		// The storage is too huge to handle, skip it but mark as incomplete.
		// For case (d), the account is resurrected might with a few slots
		// created. In this case, wipe the entire storage state diff because
		// of aborted deletion.
		if aborted {
			incomplete[addr] = struct{}{}
			delete(s.storagesOrigin, addr)
			if s.parallel.isSlotDB {
				s.parallel.storagesOriginDeleteRecord = append(s.parallel.storagesOriginDeleteRecord, addr)
			}
			continue
		}
		if s.storagesOrigin[addr] == nil {
			s.storagesOrigin[addr] = slots
		} else {
			// It can overwrite the data in s.storagesOrigin[addrHash] set by
			// 'object.updateTrie'.
			for key, val := range slots {
				s.storagesOrigin[addr][key] = val
			}
		}
		if err := nodes.Merge(set); err != nil {
			return nil, err
		}
	}
	return incomplete, nil
}

// Commit writes the state to the underlying in-memory trie database.
// Once the state is committed, tries cached in stateDB (including account
// trie, storage tries) will no longer be functional. A new state instance
// must be created with new root and updated database for accessing post-
// commit states.
//
// The associated block number of the state transition is also provided
// for more chain context.
func (s *StateDB) Commit(block uint64, deleteEmptyObjects bool) (common.Hash, error) {
	// Short circuit in case any database failure occurred earlier.
	if s.dbErr != nil {
		s.StopPrefetcher()
		return common.Hash{}, fmt.Errorf("commit aborted due to earlier error: %v", s.dbErr)
	}

	var (
		accountTrieNodesUpdated int
		accountTrieNodesDeleted int
		// Storage Tree is updated concurrently, and statistics require mutex,
		// which will affect performance, so it is discarded
		//storageTrieNodesUpdated int
		//storageTrieNodesDeleted int

		nodes      = trienode.NewMergedNodeSet()
		incomplete map[common.Address]struct{}
	)

	if !s.fullProcessed {
		s.stateRoot = s.IntermediateRoot(deleteEmptyObjects)
	}

	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			s.AccountCommits += time.Since(start)
			accountUpdatedMeter.Mark(int64(s.AccountUpdated))
			storageUpdatedMeter.Mark(int64(s.StorageUpdated))
			accountDeletedMeter.Mark(int64(s.AccountDeleted))
			storageDeletedMeter.Mark(int64(s.StorageDeleted))
			accountTrieUpdatedMeter.Mark(int64(accountTrieNodesUpdated))
			accountTrieDeletedMeter.Mark(int64(accountTrieNodesDeleted))
			//storageTriesUpdatedMeter.Mark(int64(storageTrieNodesUpdated))
			//storageTriesDeletedMeter.Mark(int64(storageTrieNodesDeleted))
			s.AccountUpdated, s.AccountDeleted = 0, 0
			s.StorageUpdated, s.StorageDeleted = 0, 0
		}(time.Now())
	}

	commitFuncs := []func() error{
		func() error {
			if metrics.EnabledExpensive {
				defer func(start time.Time) { s.TrieCommits += time.Since(start) }(time.Now())
			}
			if s.fullProcessed {
				if s.stateRoot = s.StateIntermediateRoot(); s.expectedRoot != s.stateRoot {
					log.Error("Invalid merkle root", "remote", s.expectedRoot, "local", s.stateRoot)
					return fmt.Errorf("invalid merkle root (remote: %x local: %x)", s.expectedRoot, s.stateRoot)
				}
			}
			var err error
			// Handle all state deletions first
			incomplete, err = s.handleDestruction(nodes)
			if err != nil {
				return err
			}

			tasks := make(chan func())
			type taskResult struct {
				err     error
				nodeSet *trienode.NodeSet
			}
			taskResults := make(chan taskResult, len(s.stateObjectsDirty))
			tasksNum := 0
			finishCh := make(chan struct{})

			threads := gopool.Threads(len(s.stateObjectsDirty))
			wg := sync.WaitGroup{}
			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case task := <-tasks:
							task()
						case <-finishCh:
							return
						}
					}
				}()
			}

			for addr := range s.stateObjectsDirty {
				if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
					tasks <- func() {
						// Write any storage changes in the state object to its storage trie
						if !s.noTrie {
							if set, err := obj.commit(); err != nil {
								taskResults <- taskResult{err, nil}
								return
							} else {
								taskResults <- taskResult{nil, set}
							}
						} else {
							taskResults <- taskResult{nil, nil}
						}
					}
					tasksNum++
				}
			}

			for i := 0; i < tasksNum; i++ {
				res := <-taskResults
				if res.err != nil {
					close(finishCh)
					return res.err
				}
				// Merge the dirty nodes of storage trie into global set. It is possible
				// that the account was destructed and then resurrected in the same block.
				// In this case, the node set is shared by both accounts.
				if res.nodeSet != nil {
					if err := nodes.Merge(res.nodeSet); err != nil {
						return err
					}
				}
			}
			close(finishCh)

			if !s.noTrie {
				var start time.Time
				if metrics.EnabledExpensive {
					start = time.Now()
				}
				root, set, err := s.trie.Commit(true)
				if err != nil {
					return err
				}
				// Merge the dirty nodes of account trie into global set
				if set != nil {
					if err := nodes.Merge(set); err != nil {
						return err
					}
					accountTrieNodesUpdated, accountTrieNodesDeleted = set.Size()
				}
				if metrics.EnabledExpensive {
					s.AccountCommits += time.Since(start)
				}

				origin := s.originalRoot
				if origin == (common.Hash{}) {
					origin = types.EmptyRootHash
				}

				if root != origin {
					start := time.Now()
					set := triestate.New(s.accountsOrigin, s.storagesOrigin, incomplete)
					if err := s.db.TrieDB().Update(root, origin, block, nodes, set); err != nil {
						return err
					}
					s.originalRoot = root
					if metrics.EnabledExpensive {
						s.TrieDBCommits += time.Since(start)
					}
					if s.onCommit != nil {
						s.onCommit(set)
					}
				}
			}
			wg.Wait()
			return nil
		},
		func() error {
			if metrics.EnabledExpensive {
				defer func(start time.Time) { s.CodeCommits += time.Since(start) }(time.Now())
			}
			codeWriter := s.db.DiskDB().NewBatch()
			for addr := range s.stateObjectsDirty {
				if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
					// Write any contract code associated with the state object
					if obj.code != nil && obj.dirtyCode {
						rawdb.WriteCode(codeWriter, common.BytesToHash(obj.CodeHash()), obj.code)
						obj.dirtyCode = false
						if codeWriter.ValueSize() > ethdb.IdealBatchSize {
							if err := codeWriter.Write(); err != nil {
								return err
							}
							codeWriter.Reset()
						}
					}
				}
			}
			if codeWriter.ValueSize() > 0 {
				if err := codeWriter.Write(); err != nil {
					log.Crit("Failed to commit dirty codes", "error", err)
					return err
				}
			}
			return nil
		},
		func() error {
			// If snapshotting is enabled, update the snapshot tree with this new version
			if s.snap != nil {
				if metrics.EnabledExpensive {
					defer func(start time.Time) { s.SnapshotCommits += time.Since(start) }(time.Now())
				}
				// Only update if there's a state transition (skip empty Clique blocks)
				if parent := s.snap.Root(); parent != s.expectedRoot {
					err := s.snaps.Update(s.expectedRoot, parent, s.convertAccountSet(s.stateObjectsDestruct), s.accounts, s.storages)

					if err != nil {
						log.Warn("Failed to update snapshot tree", "from", parent, "to", s.expectedRoot, "err", err)
					}

					// Keep n diff layers in the memory
					// - head layer is paired with HEAD state
					// - head-1 layer is paired with HEAD-1 state
					// - head-(n-1) layer(bottom-most diff layer) is paired with HEAD-(n-1)state
					go func() {
						if err := s.snaps.Cap(s.expectedRoot, 128); err != nil {
							log.Warn("Failed to cap snapshot tree", "root", s.expectedRoot, "layers", 128, "err", err)
						}
					}()
				}
			}
			return nil
		},
	}
	defer s.StopPrefetcher()
	commitRes := make(chan error, len(commitFuncs))
	for _, f := range commitFuncs {
		// commitFuncs[0] and commitFuncs[1] both read map `stateObjects`, but no conflicts
		tmpFunc := f
		go func() {
			commitRes <- tmpFunc()
		}()
	}
	for i := 0; i < len(commitFuncs); i++ {
		r := <-commitRes
		if r != nil {
			return common.Hash{}, r
		}
	}

	root := s.stateRoot
	s.snap = nil
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	// Clear all internal flags at the end of commit operation.
	s.accounts = make(map[common.Hash][]byte)
	s.storages = make(map[common.Hash]map[common.Hash][]byte)
	s.accountsOrigin = make(map[common.Address][]byte)
	s.storagesOrigin = make(map[common.Address]map[common.Hash][]byte)
	s.stateObjectsDirty = make(map[common.Address]struct{})
	s.stateObjectsDestruct = make(map[common.Address]*types.StateAccount)
	return root, nil
}

// Prepare handles the preparatory steps for executing a state transition with.
// This method must be invoked before state transition.
//
// Berlin fork:
// - Add sender to access list (2929)
// - Add destination to access list (2929)
// - Add precompiles to access list (2929)
// - Add the contents of the optional tx access list (2930)
//
// Potential EIPs:
// - Reset access list (Berlin)
// - Add coinbase to access list (EIP-3651)
// - Reset transient storage (EIP-1153)
func (s *StateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		s.accessList = al

		al.AddAddress(sender)
		if dst != nil {
			al.AddAddress(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			al.AddAddress(el.Address)
			for _, key := range el.StorageKeys {
				al.AddSlot(el.Address, key)
			}
		}
		if rules.IsShanghai { // EIP-3651: warm coinbase
			al.AddAddress(coinbase)
		}
	}
	// Reset transient storage at the beginning of transaction execution
	s.transientStorage = newTransientStorage()
}

// AddAddressToAccessList adds the given address to the access list
func (s *StateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.AddAddress(addr) {
		s.journal.append(accessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (s *StateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
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

// AddressInAccessList returns true if the given address is in the access list.
func (s *StateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (s *StateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return s.accessList.Contains(addr, slot)
}

// convertAccountSet converts a provided account set from address keyed to hash keyed.
func (s *StateDB) convertAccountSet(set map[common.Address]*types.StateAccount) map[common.Hash]struct{} {
	ret := make(map[common.Hash]struct{}, len(set))
	for addr := range set {
		obj, exist := s.getStateObjectFromStateObjects(addr)
		if !exist {
			ret[crypto.Keccak256Hash(addr[:])] = struct{}{}
		} else {
			ret[obj.addrHash] = struct{}{}
		}
	}
	return ret
}

// OpenStorageTrie opens the storage trie for the storage root of the provided address.
func (s *StateDB) OpenStorageTrie(addr common.Address) (Trie, error) {
	storageRoot := s.GetStorageRoot(addr)
	return s.db.OpenStorageTrie(s.originalRoot, addr, storageRoot, s.trie)
}

func (s *StateDB) NoTrie() bool {
	return s.noTrie
}

func (s *StateDB) GetSnap() snapshot.Snapshot {
	return s.snap
}

func (s *StateDB) BeforeTxTransition() {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	log.Debug("BeforeTxTransition", "mvStates", s.mvStates == nil, "rwSet", s.rwSet == nil)
	if s.mvStates == nil {
		return
	}
	s.rwSet = types.NewRWSet(types.StateVersion{
		TxIndex: s.txIndex,
	})
}

func (s *StateDB) BeginTxStat(index int) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	if s.mvStates == nil {
		return
	}
	s.es = types.NewExeStat(index).Begin()
}

func (s *StateDB) StopTxStat(usedGas uint64) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	if s.mvStates == nil {
		return
	}
	// record stat first
	if s.es != nil {
		s.es.Done().WithGas(usedGas).WithRead(len(s.rwSet.ReadSet()))
	}
}

func (s *StateDB) RecordRead(key types.RWKey, val interface{}) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	if s.mvStates == nil || s.rwSet == nil {
		return
	}
	// TODO: read from MVStates, record with ver
	s.rwSet.RecordRead(key, types.StateVersion{
		TxIndex: -1,
	}, val)
}

func (s *StateDB) RecordWrite(key types.RWKey, val interface{}) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	if s.mvStates == nil || s.rwSet == nil {
		return
	}
	s.rwSet.RecordWrite(key, val)
}

func (s *StateDB) ResetMVStates(txCount int) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	s.mvStates = types.NewMVStates(txCount)
	s.rwSet = nil
}

func (s *StateDB) FinaliseRWSet() error {
	if s.isParallel && s.parallel.isSlotDB {
		return nil
	}
	if s.mvStates == nil || s.rwSet == nil {
		return nil
	}
	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			s.TxDAGGenerate += time.Since(start)
		}(time.Now())
	}
	ver := types.StateVersion{
		TxIndex: s.txIndex,
	}
	if ver != s.rwSet.Version() {
		return errors.New("you finalize a wrong ver of RWSet")
	}

	// finalise stateObjectsDestruct
	for addr := range s.stateObjectsDestructDirty {
		s.RecordWrite(types.AccountStateKey(addr, types.AccountSuicide), struct{}{})
	}
	for addr := range s.journal.dirties {
		obj, exist := s.getStateObjectFromStateObjects(addr)
		if !exist {
			continue
		}
		if obj.selfDestructed || obj.empty() {
			// We need to maintain account deletions explicitly (will remain
			// set indefinitely). Note only the first occurred self-destruct
			// event is tracked.
			if _, ok := s.stateObjectsDestruct[obj.address]; !ok {
				log.Debug("FinaliseRWSet find Destruct", "tx", s.txIndex, "addr", addr, "selfDestructed", obj.selfDestructed)
				s.RecordWrite(types.AccountStateKey(addr, types.AccountSuicide), struct{}{})
			}
		} else {
			// finalise account & storages
			obj.finaliseRWSet()
		}
	}

	return s.mvStates.FulfillRWSet(s.rwSet, s.es)
}

func (s *StateDB) queryStateObjectsDestruct(addr common.Address) (*types.StateAccount, bool) {
	if !(s.isParallel && s.parallel.isSlotDB) {
		if acc, ok := s.stateObjectsDestructDirty[addr]; ok {
			return acc, ok
		}
	}
	acc, ok := s.stateObjectsDestruct[addr]
	return acc, ok
}

func (s *StateDB) tagStateObjectsDestruct(addr common.Address, acc *types.StateAccount) {
	if !(s.isParallel && s.parallel.isSlotDB) {
		s.stateObjectsDestructDirty[addr] = acc
		return
	}
	s.stateObjectsDestruct[addr] = acc
	return
}

func (s *StateDB) deleteStateObjectsDestruct(addr common.Address) {
	if !(s.isParallel && s.parallel.isSlotDB) {
		delete(s.stateObjectsDestructDirty, addr)
		return
	}
	delete(s.stateObjectsDestruct, addr)
}

func (s *StateDB) MVStates2TxDAG() (types.TxDAG, map[int]*types.ExeStat) {
	if s.isParallel && s.parallel.isSlotDB {
		return nil, nil
	}
	if s.mvStates == nil {
		return types.NewEmptyTxDAG(), nil
	}
	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			s.TxDAGGenerate += time.Since(start)
		}(time.Now())
	}

	return s.mvStates.ResolveTxDAG(), s.mvStates.Stats()
}

func (s *StateDB) MVStates() *types.MVStates {
	if s.isParallel && s.parallel.isSlotDB {
		return nil
	}
	return s.mvStates
}

func (s *StateDB) RecordSystemTxRWSet(index int) {
	if s.isParallel && s.parallel.isSlotDB {
		return
	}
	if s.mvStates == nil {
		return
	}
	s.mvStates.FulfillRWSet(types.NewRWSet(types.StateVersion{
		TxIndex: index,
	}).WithSerialFlag(), types.NewExeStat(index).WithSerialFlag())
}

// copySet returns a deep-copied set.
func copySet[k comparable](set map[k][]byte) map[k][]byte {
	copied := make(map[k][]byte, len(set))
	for key, val := range set {
		copied[key] = common.CopyBytes(val)
	}
	return copied
}

// copy2DSet returns a two-dimensional deep-copied set.
func copy2DSet[k comparable](set map[k]map[common.Hash][]byte) map[k]map[common.Hash][]byte {
	copied := make(map[k]map[common.Hash][]byte, len(set))
	for addr, subset := range set {
		copied[addr] = make(map[common.Hash][]byte, len(subset))
		for key, val := range subset {
			copied[addr][key] = common.CopyBytes(val)
		}
	}
	return copied
}

// PrepareForParallel prepares for state db to be used in parallel execution mode.
func (s *StateDB) PrepareForParallel() {
	s.isParallel = true
	s.parallel.stateObjects = &StateObjectSyncMap{}
	// copy objects in stateObjects into parallel if not exist.
	// This is lock free as the PrepareForParallel() is invoked at serial phase.
	for addr, objPtr := range s.stateObjects {
		if _, exist := s.parallel.stateObjects.LoadStateObject(addr); !exist {
			newObj := objPtr.deepCopy(s)
			s.parallel.stateObjects.StoreStateObject(addr, newObj)
		}
	}
}

func (s *StateDB) AddrPrefetch(slotDb *ParallelStateDB) {
	addressesToPrefetch := make([][]byte, 0, len(slotDb.parallel.dirtiedStateObjectsInSlot))
	for addr, obj := range slotDb.parallel.dirtiedStateObjectsInSlot {
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
		if obj.deleted {
			continue
		}
		// copied from obj.finalise(true)
		slotsToPrefetch := make([][]byte, 0, obj.dirtyStorage.Length())
		obj.dirtyStorage.Range(func(key, value interface{}) bool {
			originalValue, _ := obj.originStorage.GetValue(key.(common.Hash))
			if value.(common.Hash) != originalValue {
				originalKey := key.(common.Hash)
				slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(originalKey[:])) // Copy needed for closure
			}
			return true
		})
		if s.prefetcher != nil && len(slotsToPrefetch) > 0 {
			s.prefetcher.prefetch(obj.addrHash, obj.data.Root, obj.address, slotsToPrefetch)
		}
	}

	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		// log.Info("AddrPrefetch", "slotDb.TxIndex", slotDb.TxIndex(),
		//	"len(addressesToPrefetch)", len(slotDb.parallel.addressesToPrefetch))
		s.prefetcher.prefetch(common.Hash{}, s.originalRoot, emptyAddr, addressesToPrefetch)
	}
}

// MergeSlotDB is for Parallel execution mode, when the transaction has been
// finalized(dirty -> pending) on execution slot, the execution results should be
// merged back to the main StateDB.
func (s *StateDB) MergeSlotDB(slotDb *ParallelStateDB, slotReceipt *types.Receipt, txIndex int) *StateDB {
	s.SetTxContext(slotDb.thash, slotDb.txIndex)

	for s.nextRevisionId < slotDb.nextRevisionId {
		if len(slotDb.validRevisions) > 0 {
			r := slotDb.validRevisions[s.nextRevisionId]
			s.validRevisions = append(s.validRevisions, r)
		}
		s.nextRevisionId++
		if len(slotDb.validRevisions) < s.nextRevisionId {
			continue
		}
	}

	// receipt.Logs use unified log index within a block
	// align slotDB's log index to the block stateDB's logSize
	for _, l := range slotReceipt.Logs {
		l.Index += s.logSize
		s.logs[s.thash] = append(s.logs[s.thash], l)
	}

	s.logSize += slotDb.logSize

	// only merge dirty objects
	addressesToPrefetch := make([][]byte, 0, len(slotDb.stateObjectsDirty))

	for addr := range slotDb.stateObjectsDirty {
		if _, exist := s.stateObjectsDirty[addr]; !exist {
			s.stateObjectsDirty[addr] = struct{}{}
		}

		// stateObjects: KV, balance, nonce...
		dirtyObj, ok := slotDb.parallel.dirtiedStateObjectsInSlot[addr]
		if !ok {
			log.Error("parallel merge, but dirty object not exist!", "SlotIndex", slotDb.parallel.SlotIndex, "txIndex:", slotDb.txIndex, "addr", addr)
			continue
		}
		mainObj, exist := s.loadStateObj(addr)
		if !exist || mainObj.deleted {
			// addr not exist on main DB, the object is created in the merging tx.
			mainObj = dirtyObj.deepCopy(s)
			if !dirtyObj.deleted {
				mainObj.finalise(true)
			}
			s.storeStateObj(addr, mainObj)

			// fixme: should not delete, would cause unconfirmed DB incorrect?
			// delete(slotDb.parallel.dirtiedStateObjectsInSlot, addr) // transfer ownership, fixme: shared read?
			if dirtyObj.deleted {
				// remove the addr from snapAccounts&snapStorage only when object is deleted.
				// "deleted" is not equal to "snapDestructs", since createObject() will add an addr for
				//  snapDestructs to destroy previous object, while it will keep the addr in snapAccounts & snapAccounts
				s.snapParallelLock.Lock()
				delete(s.snapAccounts, addr)
				delete(s.snapStorage, addr)
				s.snapParallelLock.Unlock()
				s.AccountMux.Lock()
				delete(s.accounts, dirtyObj.addrHash)      // Clear out any previously updated account data (may be recreated via a resurrect)
				delete(s.accountsOrigin, dirtyObj.address) // Clear out any previously updated account data (may be recreated via a resurrect)
				s.AccountMux.Unlock()
				s.StorageMux.Lock()
				delete(s.storages, dirtyObj.addrHash)      // Clear out any previously updated storage data (may be recreated via a resurrect)
				delete(s.storagesOrigin, dirtyObj.address) // Clear out any previously updated storage data (may be recreated via a resurrect)
				s.StorageMux.Unlock()
			}
		} else {
			// addr already in main DB, do merge: balance, KV, code, State(create, suicide)
			// can not do copy or ownership transfer directly, since dirtyObj could have outdated
			// data(maybe updated within the conflict window)
			var newMainObj = mainObj // we don't need to copy the object since the storages are thread safe
			if createdOrChanged, ok := slotDb.parallel.addrStateChangesInSlot[addr]; ok {
				// there are 4 kinds of state change:
				// 1.Suicide
				// 2.Empty Delete
				// 3.createObject
				//   a: AddBalance,SetState to a non-exist or deleted(suicide, empty delete) address.
				//   b: CreateAccount: like DAO the fork, regenerate an account carry its balance without KV
				// 4. setState.
				// For these state change
				if createdOrChanged {
					// Need to differentiate the case of createObject and setState, since the mainDB at this moment contains
					// the latest update of the object, which cause the object.data.root newer then the dirtyObject. so
					// the deepCopy() here can not be used for setState as it introduces issue that the pendingStorage
					// may not empty until block validation. so the pendingStorage filled by the execution of previous txs
					// in same block may get overwritten by deepCopy here, which causes issue in root calculation.
					if _, created := s.parallel.createdObjectRecord[addr]; created {
						newMainObj = dirtyObj.deepCopy(s)
					} else {
						// Merge the dirtyObject with mainObject
						if _, balanced := slotDb.parallel.balanceChangesInSlot[addr]; balanced {
							newMainObj.dirtyBalance = dirtyObj.dirtyBalance
							newMainObj.data.Balance = dirtyObj.data.Balance
						}
						if _, coded := slotDb.parallel.codeChangesInSlot[addr]; coded {
							newMainObj.code = dirtyObj.code
							newMainObj.dirtyCodeHash = dirtyObj.dirtyCodeHash
							newMainObj.data.CodeHash = dirtyObj.data.CodeHash
							newMainObj.dirtyCode = true
						}
						if keys, stated := slotDb.parallel.kvChangesInSlot[addr]; stated {
							newMainObj.MergeSlotObject(s.db, dirtyObj, keys)
						}
						if _, nonced := slotDb.parallel.nonceChangesInSlot[addr]; nonced {
							// dirtyObj.Nonce() should not be less than newMainObj
							newMainObj.data.Nonce = dirtyObj.data.Nonce
							newMainObj.dirtyNonce = dirtyObj.dirtyNonce
						}
						newMainObj.deleted = dirtyObj.deleted
					}
				} else {
					// The object is deleted in the TX.
					newMainObj = dirtyObj.deepCopy(s)
				}

				// All cases with addrStateChange set to true/false can be deleted. so handle it here.
				if dirtyObj.deleted {
					// remove the addr from snapAccounts&snapStorage only when object is deleted.
					// "deleted" is not equal to "snapDestructs", since createObject() will add an addr for
					//  snapDestructs to destroy previous object, while it will keep the addr in snapAccounts & snapAccounts
					s.snapParallelLock.Lock()
					delete(s.snapAccounts, addr)
					delete(s.snapStorage, addr)
					s.snapParallelLock.Unlock()
					s.AccountMux.Lock()
					delete(s.accounts, dirtyObj.addrHash)      // Clear out any previously updated account data (may be recreated via a resurrect)
					delete(s.accountsOrigin, dirtyObj.address) // Clear out any previously updated account data (may be recreated via a resurrect)
					s.AccountMux.Unlock()
					s.StorageMux.Lock()
					delete(s.storages, dirtyObj.addrHash)      // Clear out any previously updated storage data (may be recreated via a resurrect)
					delete(s.storagesOrigin, dirtyObj.address) // Clear out any previously updated storage data (may be recreated via a resurrect)
					s.StorageMux.Unlock()
				}
			} else {
				// deepCopy a temporary *stateObject for safety, since slot could read the address,
				// dispatch should avoid overwrite the StateObject directly otherwise, it could
				// crash for: concurrent map iteration and map write
				// As there is dirtyBalance, Nonce and codehash, we keep it to mainObj and leave the merging work
				// to "mainObj.finalise()", just in case that newMainObj.delete == true and somewhere potentially
				// access the Nonce, balance or codehash later.
				if _, balanced := slotDb.parallel.balanceChangesInSlot[addr]; balanced {
					newMainObj.dirtyBalance = dirtyObj.dirtyBalance
					newMainObj.data.Balance = dirtyObj.data.Balance
				}
				if _, coded := slotDb.parallel.codeChangesInSlot[addr]; coded {
					newMainObj.code = dirtyObj.code
					newMainObj.dirtyCodeHash = dirtyObj.dirtyCodeHash
					newMainObj.data.CodeHash = dirtyObj.data.CodeHash
					newMainObj.dirtyCode = true
				}
				if keys, stated := slotDb.parallel.kvChangesInSlot[addr]; stated {
					newMainObj.MergeSlotObject(s.db, dirtyObj, keys)
				}
				if _, nonced := slotDb.parallel.nonceChangesInSlot[addr]; nonced {
					// dirtyObj.Nonce() should not be less than newMainObj
					newMainObj.data.Nonce = dirtyObj.data.Nonce
					newMainObj.dirtyNonce = dirtyObj.dirtyNonce
				}
				newMainObj.deleted = dirtyObj.deleted
			}
			if !newMainObj.deleted {
				newMainObj.finalise(true) // true: prefetch on dispatcher
			}
			// update the object
			s.storeStateObj(addr, newMainObj)
		}
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
	}

	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(common.Hash{}, s.originalRoot, emptyAddr, addressesToPrefetch) // prefetch for trie node of account
	}

	for addr := range slotDb.stateObjectsPending {
		if _, exist := s.stateObjectsPending[addr]; !exist {
			s.stateObjectsPending[addr] = struct{}{}
		}
	}

	for addr := range slotDb.stateObjectsDestruct {
		if acc, exist := s.stateObjectsDestruct[addr]; !exist {
			s.stateObjectsDestruct[addr] = acc
		}
	}
	// slotDb.logs: logs will be kept in receipts, no need to do merge
	for hash, preimage := range slotDb.preimages {
		s.preimages[hash] = preimage
	}
	if s.accessList != nil && slotDb.accessList != nil {
		s.accessList = slotDb.accessList.Copy()
	}

	if slotDb.snaps != nil {
		for k := range slotDb.snapDestructs {
			// There could be a race condition for parallel transaction execution
			// One transaction add balance 0 to an empty address, will delete it(delete empty is enabled).
			// While another concurrent transaction could add a none-zero balance to it, make it not empty
			// We fixed it by add an addr state read record for add balance 0
			s.snapParallelLock.Lock()
			s.snapDestructs[k] = struct{}{}
			s.snapParallelLock.Unlock()
		}
	}

	return s
}

func (s *StateDB) ParallelMakeUp(common.Address, []byte) {
	// do nothing, this API is for parallel mode
}
