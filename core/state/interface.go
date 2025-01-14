// Copyright 2016 The go-ethereum Authors
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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// StateDBer is copied from vm/interface.go
// It is used by StateObject & Journal right now, to abstract StateDB & ParallelStateDB
type StateDBer interface {
	getBaseStateDB() *StateDB
	getStateObject(common.Address) *stateObject // only accessible for journal
	storeStateObj(common.Address, *stateObject) // only accessible for journal

	CreateAccount(common.Address)

	SubBalance(common.Address, *uint256.Int)
	AddBalance(common.Address, *uint256.Int)
	GetBalance(common.Address) *uint256.Int

	GetNonce(common.Address) uint64
	SetNonce(common.Address, uint64)

	GetCodeHash(common.Address) common.Hash
	GetCode(common.Address) []byte
	SetCode(common.Address, []byte)
	GetCodeSize(common.Address) int

	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	GetCommittedState(common.Address, common.Hash) common.Hash
	GetState(common.Address, common.Hash) common.Hash
	SetState(common.Address, common.Hash, common.Hash)

	SelfDestruct(common.Address)
	HasSelfDestructed(common.Address) bool

	// Exist reports whether the given account exists in state.
	// Notably this should also return true for suicided accounts.
	Exist(common.Address) bool
	// Empty returns whether the given account is empty. Empty
	// is defined according to EIP161 (balance = nonce = code = 0).
	Empty(common.Address) bool

	//PrepareAccessList(sender common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList)
	AddressInAccessList(addr common.Address) bool
	SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool)
	// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddAddressToAccessList(addr common.Address)
	// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddSlotToAccessList(addr common.Address, slot common.Hash)

	RevertToSnapshot(int)
	Snapshot() int

	AddLog(*types.Log)
	AddPreimage(common.Hash, []byte)
	GetPreimage(common.Hash) ([]byte, bool)
	StopPrefetcher()
	StartPrefetcher(namespace string)
	SetExpectedStateRoot(root common.Hash)
	IntermediateRoot(deleteEmptyObjects bool) common.Hash
	Error() error
	Timers() *Timers
	Preimages() map[common.Hash][]byte
	Commit(block uint64, deleteEmptyObjects bool) (common.Hash, error)
	SetBalance(addr common.Address, amount *uint256.Int)
	PrepareForParallel()
	Finalise(deleteEmptyObjects bool)
	MarkFullProcessed()
	AccessListCopy() *accessList
	SetTxContext(hash common.Hash, index int)
	SetAccessList(list *accessList)
	getDeletedStateObject(addr common.Address) *stateObject
	appendJournal(journalEntry journalEntry)
	addJournalDirty(address common.Address)
	getPrefetcher() *triePrefetcher
	getDB() Database
	getOriginalRoot() common.Hash
	getTrie() Trie
	getStateObjectDestructLock() *sync.RWMutex
	getStateObjectsDestruct(addr common.Address) (*types.StateAccount, bool)
	getSnap() snapshot.Snapshot
	timeAddSnapshotStorageReads(du time.Duration)
	setError(err error)
	getTrieParallelLock() *sync.Mutex
	timeAddStorageReads(du time.Duration)
	timeAddStorageUpdates(du time.Duration)
	countAddStorageDeleted(diff int)
	countAddStorageUpdated(diff int)
	getStorageMux() *sync.Mutex
	getStorages(hash common.Hash) map[common.Hash][]byte
	setStorages(hash common.Hash, storage map[common.Hash][]byte)
	getStoragesOrigin(address common.Address) map[common.Hash][]byte
	setStoragesOrigin(address common.Address, origin map[common.Hash][]byte)
	timeAddStorageHashes(du time.Duration)
	timeAddStorageCommits(du time.Duration)
	getOrNewStateObject(addr common.Address) *stateObject
	prefetchAccount(address common.Address)
	CheckFeeReceiversRWSet()
}
