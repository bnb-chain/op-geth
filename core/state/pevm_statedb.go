package state

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
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

// UncommittedDB is a wrapper of StateDB, which records all the writes of the state.
// It is designed for parallel running of the EVM.
// All fields of UncommittedDB survive in the scope of a transaction.
// Here is the original score of each field in the maindb(StateDB):
// |------------------|---------|---------------------|------------------------|
// |  field           |  score  |  need to be merged  |  need to be committed  |
// |------------------|---------|---------------------|------------------------|
// | accessList       |  block: before Berlin hardfork ; transaction : after Berlin hardfork |  no  |  no  |
// | transientStorage | transaction |  no   |  no   |
// | objects          | block   |  yes  |  yes  |
// | refund           | block   |  yes  |  yes  |
// | preimages        | block   |  yes   |  yes   |
// |------------------|---------|-------|-------|
type UncommittedDB struct {
	isBerlin  bool
	discarded bool

	// transaction context
	txHash  common.Hash
	txIndex int
	logs    []*types.Log

	// accessList
	accessList *accessList

	// in the maindb, transientStorage survives only in the scope of a transaction, and no need to
	//@todo make transientStorage lazy loaded
	transientStorage transientStorage

	// object reads and writes
	reads reads
	cache writes

	// in the maindb, refund survives in the scope of block; and will be reset to 0 whenever a transaction's modification is Finalized to trie .
	prevRefund *uint64
	refund     uint64

	// in the maindb, preimages survives in the scope of block
	// it might be too large that we need to lazy load it.
	preimages      map[common.Hash][]byte
	preimagesReads map[common.Hash][]byte

	journal ujournal

	maindb               StateDBer
	isMainDBIsParallelDB bool
}

func (pst *UncommittedDB) CheckFeeReceiversRWSet() {
	//TODO implement me
	return
}

func NewUncommittedDB(maindb StateDBer) *UncommittedDB {
	_, ok := maindb.(*ParallelStateDB)
	return &UncommittedDB{
		accessList:           newAccessList(),
		transientStorage:     newTransientStorage(),
		maindb:               maindb,
		reads:                make(reads),
		cache:                make(writes),
		isMainDBIsParallelDB: ok,
	}
}

// @TODO drop?
func (pst *UncommittedDB) BeforeTxTransition() {
}

// @TODO drop?
func (pst *UncommittedDB) FinaliseRWSet() error {
	return nil
}

// @TODO drop?
func (pst *UncommittedDB) TxIndex() int {
	return pst.txIndex
}

func (pst *UncommittedDB) SetTxContext(txHash common.Hash, txIndex int) {
	pst.txHash = txHash
	pst.txIndex = txIndex
}

// ===============================================
// Constructor
func (pst *UncommittedDB) CreateAccount(addr common.Address) {
	pst.journal.append(newJCreateAccount(pst.cache[addr], addr))
	obj := pst.getDeletedObject(addr, pst.maindb)
	// keep the balance
	pst.cache.create(addr)
	if obj != nil {
		pst.cache[addr].balance.Set(obj.balance)
	}
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
func (pst *UncommittedDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	// do nothing, because the work had been done in constructer NewUncommittedDB()
	// 0. init accessList
	// 1. init transientStorage
	pst.isBerlin = rules.IsBerlin
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		pst.accessList = al

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
	} else {
		pst.accessList = pst.maindb.AccessListCopy()
	}
	// Reset transient storage at the beginning of transaction execution
	pst.transientStorage = newTransientStorage()
}

// ===============================================
// Object Methods
//  1. journal
//  2. object

func (pst *UncommittedDB) SubBalance(addr common.Address, amount *uint256.Int) {
	if amount.IsZero() {
		obj := pst.getOrNewObject(addr)
		if !obj.empty(pst) {
			return
		}
	}
	pst.journal.append(newJBalance(pst.cache[addr], addr))
	obj := pst.getOrNewObject(addr)
	newb := new(uint256.Int).Sub(obj.balance, amount)
	pst.cache.setBalance(addr, newb)
}

func (pst *UncommittedDB) AddBalance(addr common.Address, amount *uint256.Int) {
	if amount.IsZero() {
		obj := pst.getOrNewObject(addr)
		if !obj.empty(pst) {
			return
		}
	}
	pst.journal.append(newJBalance(pst.cache[addr], addr))
	obj := pst.getOrNewObject(addr)
	newb := new(uint256.Int).Add(obj.balance, amount)
	pst.cache.setBalance(addr, newb)
}

func (pst *UncommittedDB) GetBalance(addr common.Address) *uint256.Int {
	if obj := pst.getObject(addr); obj != nil {
		return new(uint256.Int).Set(obj.balance)
	}
	return uint256.NewInt(0)
}

func (pst *UncommittedDB) GetNonce(addr common.Address) uint64 {
	if obj := pst.getObject(addr); obj != nil {
		return obj.nonce
	}
	return 0
}

func (pst *UncommittedDB) SetNonce(addr common.Address, nonce uint64) {
	pst.journal.append(newJNonce(pst.cache[addr], addr))
	pst.getOrNewObject(addr)
	pst.cache.setNonce(addr, nonce)
}

func (pst *UncommittedDB) GetCodeHash(addr common.Address) common.Hash {
	obj := pst.getDeletedObjectWithCode(addr, pst.maindb)
	if obj == nil || obj.deleted {
		return common.Hash{}
	}
	return common.BytesToHash(obj.codeHash)
}

func (pst *UncommittedDB) GetCode(addr common.Address) []byte {
	obj := pst.getDeletedObjectWithCode(addr, pst.maindb)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.code
}

func (pst *UncommittedDB) GetCodeSize(addr common.Address) int {
	obj := pst.getDeletedObjectWithCode(addr, pst.maindb)
	if obj == nil || obj.deleted {
		return 0
	}
	return obj.codeSize
}

func (pst *UncommittedDB) SetCode(addr common.Address, code []byte) {
	pst.journal.append(newJCode(pst.cache[addr], addr))
	if obj := pst.getDeletedObjectWithCode(addr, pst.maindb); obj == nil || obj.deleted {
		pst.journal.append(newJCreateAccount(pst.cache[addr], addr))
		pst.cache.create(addr)
	}
	pst.cache.setCode(addr, code)
}

func (pst *UncommittedDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	// this is a uncommitted db, so just get it from the maindb
	//@TODO GetCommittedState() need to be thread safe
	return pst.maindb.GetCommittedState(addr, hash)
}

func (pst *UncommittedDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	obj := pst.getDeletedObjectWithState(addr, pst.maindb, hash)
	if obj == nil || obj.deleted {
		return common.Hash{}
	}
	return obj.state[hash]
}

func (pst *UncommittedDB) SetState(addr common.Address, key, value common.Hash) {
	pst.journal.append(newJStorage(pst.cache[addr], addr, key))
	if obj := pst.getDeletedObjectWithState(addr, pst.maindb, key); obj == nil || obj.deleted {
		pst.journal.append(newJCreateAccount(pst.cache[addr], addr))
		pst.cache.create(addr)
	}
	pst.cache.setState(addr, key, value)
}

func (pst *UncommittedDB) SelfDestruct(addr common.Address) {
	pst.journal.append(newJSelfDestruct(pst.cache[addr]))
	if obj := pst.getObject(addr); obj == nil {
		return
	}
	pst.cache.selfDestruct(addr)
}

func (pst *UncommittedDB) HasSelfDestructed(addr common.Address) bool {
	if obj := pst.getObject(addr); obj != nil {
		return obj.selfDestruct
	}
	return false
}

func (pst *UncommittedDB) Selfdestruct6780(addr common.Address) {
	obj := pst.getObject(addr)
	if obj == nil {
		return
	}
	if obj.created {
		pst.SelfDestruct(addr)
	}
}

func (pst *UncommittedDB) Exist(addr common.Address) bool {
	return pst.getObject(addr) != nil
}

func (pst *UncommittedDB) Empty(addr common.Address) bool {
	obj := pst.getObject(addr)
	return obj == nil || obj.empty(pst)
}

// ===============================================
//  Refund Methods
// 	1. journal
//  2. refunds

func (pst *UncommittedDB) AddRefund(gas uint64) {
	pst.journal.append(newJRefund(pst.refund))
	pst.recordRefundOnce()
	pst.refund += gas
}

func (pst *UncommittedDB) SubRefund(gas uint64) {
	pst.journal.append(newJRefund(pst.refund))
	pst.recordRefundOnce()
	if pst.refund < gas {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, pst.refund))
	}
	pst.refund -= gas
}

func (pst *UncommittedDB) GetRefund() uint64 {
	pst.recordRefundOnce()
	return pst.refund
}

func (pst *UncommittedDB) recordRefundOnce() {
	if pst.prevRefund == nil {
		refund := pst.maindb.GetRefund()
		pst.prevRefund = &refund
		pst.refund = refund
	}
}

// ===============================================
// transientStorage Methods (EIP-1153: https://eips.ethereum.org/EIPS/eip-1153)

func (pst *UncommittedDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	return pst.transientStorage.Get(addr, key)
}
func (pst *UncommittedDB) SetTransientState(addr common.Address, key, value common.Hash) {
	prev := pst.transientStorage.Get(addr, key)
	if prev == value {
		return // no need to record the same value
	}
	pst.journal.append(newJTransientStorage(addr, key, prev))
	pst.transientStorage.Set(addr, key, value)
}

// ===============================================
// AccessList Methods (EIP-2930: https://eips.ethereum.org/EIPS/eip-2930)
func (pst *UncommittedDB) AddressInAccessList(addr common.Address) bool {
	return pst.accessList.ContainsAddress(addr)
}
func (pst *UncommittedDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	return pst.accessList.Contains(addr, slot)
}
func (pst *UncommittedDB) AddAddressToAccessList(addr common.Address) {
	if pst.accessList.AddAddress(addr) {
		pst.journal.append(&jAccessList{addr: &addr})
	}
}
func (pst *UncommittedDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrChanged, slotChanged := pst.accessList.AddSlot(addr, slot)
	if addrChanged {
		pst.journal.append(&jAccessList{addr: &addr})
	}
	if slotChanged {
		pst.journal.append(&jAccessListSlot{addr: &addr, slot: &slot})
	}
}

// ===============================================
// Snapshot Methods
func (pst *UncommittedDB) RevertToSnapshot(id int) {
	if id < 0 || id > len(pst.journal) {
		panic(fmt.Sprintf("invalid snapshot index, out of range, snapshot:%d, len:%d", id, len(pst.journal)))
	}
	pst.journal.revertTo(pst, id)
}
func (pst *UncommittedDB) Snapshot() int {
	return pst.journal.snapshot()
}

// ===============================================
// Logs Methods
func (pst *UncommittedDB) AddLog(log *types.Log) {
	pst.journal.append(&jLog{i: uint(len(pst.logs))})
	log.TxHash = pst.txHash
	log.TxIndex = uint(pst.txIndex)
	// we don't need to set Index now, because it will be recalculated when merging into maindb
	log.Index = uint(len(pst.logs))
	pst.logs = append(pst.logs, log)
}

// PackLogs returns the logs matching the specified transaction hash, and annotates
// them with the given blockNumber and blockHash.
func (s *UncommittedDB) PackLogs(blockNumber uint64, blockHash common.Hash) []*types.Log {
	for _, l := range s.logs {
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return s.logs
}

// ===============================================
// Preimage Methods (EIP-1352: https://eips.ethereum.org/EIPS/eip-1352)
func (pst *UncommittedDB) AddPreimage(hash common.Hash, preimage []byte) {
	pst.journal.append(newJPreimage(hash))
	pst.recordPreimageOnce(hash)
	if _, ok := pst.preimages[hash]; !ok {
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		pst.preimages[hash] = pi
	}
}

func (pst *UncommittedDB) recordPreimageOnce(hash common.Hash) {
	// first time to read the preimage
	if pst.preimages == nil {
		// load all preimages from maindb
		pst.preimages = make(map[common.Hash][]byte)
		pst.preimagesReads = make(map[common.Hash][]byte)
		for h, pi := range pst.maindb.Preimages() {
			pst.preimagesReads[h], pst.preimages[h] = pi, pi
		}
	}
	if _, ok := pst.preimagesReads[hash]; ok {
		return
	}
	// get and record the preimage state from the maindb
	preimageFromMaindb, ok := pst.maindb.GetPreimage(hash)
	if ok {
		pst.preimagesReads[hash] = preimageFromMaindb
	} else {
		// not exists in the maindb too
		pst.preimagesReads[hash] = nil
	}
}

func (pst *UncommittedDB) Preimages() map[common.Hash][]byte {
	return pst.preimages
}

// check conflict
func (pst *UncommittedDB) conflictsToMaindb() error {
	// 1. check preimages reads conflict (@TODO preimage should be lazy loaded)
	// 2. check accesslist reads conflict (@TODO confirm: whether the accesslist should be checked or not)
	// 3. check logs conflict (check the txIndex)
	// 4. check object conflict
	stateDB, ok := pst.maindb.(*StateDB)
	if !ok {
		return errors.New("should not to check conflicts when the Maindb is a StateDB")
	}
	if err := pst.hasLogConflict(stateDB); err != nil {
		return err
	}
	if err := pst.hasRefundConflict(stateDB); err != nil {
		return err
	}
	if err := pst.hasPreimageConflict(stateDB); err != nil {
		return err
	}
	return pst.reads.conflictsTo(stateDB)
}

func (pst *UncommittedDB) hasLogConflict(maindb *StateDB) error {
	if pst.txIndex == 0 {
		// this is the first transaction in the block,
		// so the logs should be empty
		// and the maindb.txIndex should be -1
		if maindb.logSize != 0 {
			return fmt.Errorf("conflict logs, logSize: %d, expected 0", maindb.logSize)
		}
		if len(maindb.logs) != 0 {
			return fmt.Errorf("conflict logs, logsLen: %d, expected 0", len(maindb.logs))
		}
	} else {
		// this is not the first transaction in the block
		if maindb.txIndex != int(pst.txIndex)-1 {
			return fmt.Errorf("conflict txIndex, txIndex: %d, expected %d", maindb.txIndex, pst.txIndex-1)
		}
	}
	return nil
}

func (pst *UncommittedDB) hasPreimageConflict(maindb *StateDB) error {
	for hash, preimage := range pst.preimagesReads {
		mainstate := maindb.preimages[hash]
		if !bytes.Equal(preimage, mainstate) {
			return fmt.Errorf("conflict preimage, hash:%s, expected:%s, actual:%s", hash.String(), preimage, mainstate)
		}
	}
	return nil
}

func (pst *UncommittedDB) hasRefundConflict(maindb *StateDB) error {
	// never read
	if pst.prevRefund == nil {
		return nil
	}
	if *pst.prevRefund != maindb.GetRefund() {
		return fmt.Errorf("conflict refund, expected:%d, actual:%d", *pst.prevRefund, maindb.GetRefund())
	}
	return nil
}

func (pst *UncommittedDB) ConflictsToMaindb() error {
	return pst.conflictsToMaindb()
}

func (pst *UncommittedDB) Merge(deleteEmptyObjects bool) error {
	if pst.discarded {
		// all the writes of this db will be discarded, including:
		// 1. accessList
		// 2. preimages
		// 3. obj states
		// 4. refund
		// so we don't need to merge anything.
		return nil
	}

	// 0. set the TxContext
	pst.maindb.SetTxContext(pst.txHash, pst.txIndex)
	// 1. merge preimages writes
	for hash, preimage := range pst.preimages {
		pst.maindb.AddPreimage(hash, preimage)
	}
	// 2. merge accesslist writes
	// we need to merge accessList before Berlin hardfork
	if !pst.isBerlin {
		for addr, slot := range pst.accessList.addresses {
			pst.maindb.AddAddressToAccessList(addr)
			if slot >= 0 {
				slots := pst.accessList.slots[slot]
				for hash := range slots {
					pst.maindb.AddSlotToAccessList(addr, hash)
				}
			}
		}
	} else {
		// after Berlin hardfork, all the accessList should be reset before a transaction was executed
		pst.maindb.SetAccessList(pst.accessList)
	}
	// 3. merge logs writes
	for _, st := range pst.cache {
		st.merge(pst.maindb, pst.isMainDBIsParallelDB)
	}
	// 4. merge object states
	for _, log := range pst.logs {
		if pst.isMainDBIsParallelDB {
			pst.maindb.(*ParallelStateDB).AddLogWithTx(log, pst.txHash, uint(pst.txIndex))
		} else {
			pst.maindb.AddLog(log)
		}
	}
	// 5. merge refund
	if pst.refund != 0 {
		pst.maindb.AddRefund(pst.refund)
	}
	//// clean empty objects if needed
	//for _, obj := range pst.cache {
	//	if obj.selfDestruct || (deleteEmptyObjects && obj.empty(pst)) {
	//		obj.deleted = true
	//	}
	//	// we don't need to do obj.finalize() here, it will be done in the maindb.Finalize()
	//	// just mark the object as deleted
	//	obj.created = false
	//}
	return nil
}

func (pst *UncommittedDB) Finalise(deleteEmptyObjects bool) {
	pst.maindb.Finalise(deleteEmptyObjects)
}

// getDeletedObj returns the state object for the given address or nil if not found.
// it first gets from the maindb, if not found, then get from the maindb.
// it never modifies the maindb. the maindb is read-only.
// there are some cases to be handle:
//  1. it exists in the maindb's cache
//     a) just return it
//  2. it exists in the maindb, but not in the cache:
//     a) record a read state
//     b) clone it, and put it into the cache
//     c) return it
//  3. it doesn't exist in the maindb, and neighter in the slaveb:
//     a) record a read state with create = true
//     b) return nil
//
// it is notable that the getObj() will not be called parallelly, but the getStateObject() of maindb will be.
// so we need:
//  1. the uncommittedDB's cache is not thread safe
//  2. the maindb's cache is thread safe
func (pst *UncommittedDB) getDeletedObject(addr common.Address, maindb StateDBer) (o *state) {
	if pst.cache[addr] != nil {
		return pst.cache[addr]
	}
	// it reads the cache from the maindb
	obj := maindb.getDeletedStateObject(addr)
	defer func() {
		pst.reads.recordOnce(addr, copyObj(obj))
	}()
	if obj == nil {
		// the object is not found, do a read record
		return nil
	}
	// write it into the cache and return
	pst.cache[addr] = copyObj(obj)
	return pst.cache[addr]
}

// getObject returns the state object for the given address or nil if not found.
//  1. it first gets from the cache.
//  2. if not found, then get from the maindb,
//  3. record its state in maindb for the first read, which is for further conflict check.
func (pst *UncommittedDB) getObject(addr common.Address) *state {
	obj := pst.getDeletedObject(addr, pst.maindb)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj
}

// getOrNewObj returns the state object for the given address or create a new one if not found.
//  1. it first gets from the cache.
//  2. if not found, then get from the maindb:
//     2.1) if not found
//     2.1.1) create a new one in the cache, keep maindb unchanged
//     2.1.2) record a read state with create = true
//     2.1.3) record a write cache with create = true, balance = 0
//     2.2) if found, record its state in maindb for the first read, which is for further conflict check.
//
// 3. return the state object
func (pst *UncommittedDB) getOrNewObject(addr common.Address) *state {
	obj := pst.getObject(addr)
	if obj != nil {
		return obj
	}
	return pst.cache.create(addr)
}

func (pst *UncommittedDB) getObjectWithCode(addr common.Address) *state {
	obj := pst.getObject(addr)
	if obj == nil {
		return nil
	}
	if obj.codeIsLoaded() {
		return obj
	}
	// try to load the code from maindb
	deletedObject := pst.maindb.getStateObject(addr)
	if deletedObject == nil {
		return nil
	}
	code, codeHash := deletedObject.Code(), common.BytesToHash(deletedObject.CodeHash())
	obj.code = code
	obj.codeHash = codeHash[:]
	obj.codeSize = len(code)
	return obj
}

// getDeletedObjectWithCode return an object with code, and load the code from maindb if it is not loaded.
func (pst *UncommittedDB) getDeletedObjectWithCode(addr common.Address, maindb StateDBer) (o *state) {
	o = pst.getDeletedObject(addr, maindb)
	if o == nil {
		pst.reads.recordCodeOnce(addr, common.Hash{}, nil)
		return nil
	}
	if o.codeIsLoaded() {
		return o
	}
	// load code from maindb
	deletedObj := pst.maindb.getDeletedStateObject(addr)
	var code = []byte{}
	var codeHash = common.Hash{}
	if deletedObj == nil {
		pst.reads.recordCodeOnce(addr, common.Hash{}, nil)
	} else {
		pst.reads.recordCodeOnce(addr, common.BytesToHash(deletedObj.CodeHash()), deletedObj.Code())
		// set code into the cache
		code, codeHash = deletedObj.Code(), common.BytesToHash(deletedObj.CodeHash())
	}
	o.code = code
	o.codeHash = codeHash[:]
	o.codeSize = len(code)
	return o
}

// getDeletedObjectWithState return an object with state, and load the state from maindb if it is not loaded.
func (pst *UncommittedDB) getDeletedObjectWithState(addr common.Address, maindb StateDBer, hash common.Hash) (o *state) {
	o = pst.getDeletedObject(addr, maindb)
	if o == nil {
		pst.reads.recordKVOnce(addr, hash, common.Hash{})
		return nil
	}
	if _, ok := o.state[hash]; ok {
		return o
	}
	// first, load code from maindb and record the previous state
	// we can't use getStateObject() here , because the state of deletedObj will be used for conflict check.
	deletedObj := pst.maindb.getDeletedStateObject(addr)
	if deletedObj != nil {
		// record the previous state for conflict check.
		pst.reads.recordKVOnce(addr, hash, deletedObj.GetState(hash))
	}

	// now write the true state into cache
	var value = common.Hash{}
	if deletedObj != nil && !deletedObj.deleted {
		value = deletedObj.GetState(hash)
	}
	o.state[hash] = value
	return o
}

type state struct {
	// object states
	modified int32 //records all the modified fields
	addr     common.Address
	balance  *uint256.Int
	nonce    uint64
	//@TODO code is lazy loaded, be careful to record its state
	code         []byte
	codeHash     []byte
	codeSize     int // when codeSize == -1, it means the code is unloaded
	state        map[common.Hash]common.Hash
	selfDestruct bool
	created      bool
	deleted      bool
}

func (c *state) clone() *state {
	newstate := make(map[common.Hash]common.Hash, len(c.state))
	for k, v := range c.state {
		newstate[k] = v
	}
	return &state{
		modified:     c.modified,
		addr:         c.addr,
		balance:      new(uint256.Int).Set(c.balance),
		nonce:        c.nonce,
		code:         append([]byte(nil), c.code...),
		codeHash:     append([]byte(nil), c.codeHash...),
		codeSize:     c.codeSize,
		state:        newstate,
		selfDestruct: c.selfDestruct,
		created:      c.created,
		deleted:      c.deleted,
	}
}

func (s *state) markAllModified() {
	s.modified |= ModifyBalance
	s.modified |= ModifyNonce
	// @TODO confirm: whether the code should be reset or not?
	// @TODO confirm: whether the state should be reset or not?
	s.modified |= ModifyCode
	s.modified |= ModifyState
}

// check whether the state of current object is conflicted with the maindb
func (s state) conflicts(maindb *StateDB) error {
	addr := s.addr
	obj := maindb.getDeletedStateObject(addr)
	// created == true means it doen't exist in the maindb
	if s.created != (obj == nil) {
		return errors.New("conflict: created")
	}
	// newly created object, no need to compare anything
	if obj == nil {
		return nil
	}
	// they are all deleted, no need to compare anything
	//if obj.deleted && s.deleted {
	//	return nil
	//}
	if s.deleted != obj.deleted {
		return fmt.Errorf("conlict: deleted, expected:%t, actual:%t", s.deleted, obj.deleted)
	}
	if s.selfDestruct != obj.selfDestructed {
		return fmt.Errorf("conflict: selfDestruct, expected:%t, actual:%t", s.selfDestruct, obj.selfDestructed)
	}
	if s.nonce != obj.Nonce() {
		return fmt.Errorf("conflict: nonce, expected:%d, actual:%d", s.nonce, obj.Nonce())
	}
	if s.balance.Cmp(obj.Balance()) != 0 {
		return fmt.Errorf("conflict: balance, expected:%d, actual:%d", s.balance.Uint64(), obj.data.Balance.Uint64())
	}
	// code is lazy loaded
	if s.codeIsLoaded() && !bytes.Equal(obj.Code(), s.code) {
		return fmt.Errorf("conflict: code, expected len:%d, actual len:%d", len(s.code), len(obj.Code()))
	}
	// state is lazy loaded, and should be checked as less as possible , too.
	for key, val := range s.state {
		if obj.GetState(key).Cmp(val) != 0 {
			return fmt.Errorf("conflict: state, key:%s, expected:%s, actual:%s", key.String(), val.String(), obj.GetState(key).String())
		}
	}
	return nil
}

func (s state) merge(maindb StateDBer, prefetch bool) {
	// 1. merge the balance
	// 2. merge the nonce
	// 3. merge the code
	// 4. merge the state
	if s.modified&ModifySelfDestruct != 0 {
		maindb.SelfDestruct(s.addr)
		if prefetch {
			maindb.prefetchAccount(s.addr)
		}
		return
	}
	hasModified := false
	obj := maindb.getOrNewStateObject(s.addr)
	if s.modified&ModifyBalance != 0 {
		obj.SetBalance(s.balance)
		hasModified = true
	}
	if s.modified&ModifyNonce != 0 {
		obj.SetNonce(s.nonce)
		hasModified = true
	}
	if s.modified&ModifyCode != 0 {
		obj.SetCode(common.BytesToHash(s.codeHash), s.code)
		hasModified = true
	}
	if s.modified&ModifyState != 0 {
		for key, val := range s.state {
			obj.SetState(key, val)
			if prefetch {
				obj.prefetchStorage(key, val)
			}
		}
		hasModified = true
		//TODO: should we reset all kv pairs if the s.state == nil ?
	}
	if hasModified && prefetch {
		maindb.prefetchAccount(obj.address)
	}
}

func (s *state) empty(pst *UncommittedDB) bool {
	return s.nonce == 0 && s.balance.Sign() == 0 && s.codeIsEmpty(pst)
}

func (s *state) codeIsEmpty(pst *UncommittedDB) bool {
	if !s.codeIsLoaded() {
		codeState := pst.getDeletedObjectWithCode(s.addr, pst.maindb)
		if codeState == nil {
			return true
		}
		s.code = codeState.code
		s.codeHash = codeState.codeHash
		s.codeSize = codeState.codeSize
	}
	return bytes.Equal(s.codeHash, types.EmptyCodeHash.Bytes()) || bytes.Equal(s.code, common.Hash{}.Bytes())
}

func (s *state) codeIsLoaded() bool {
	return s.codeSize != -1
}

func copyObj(obj *stateObject) *state {
	if obj == nil {
		return nil
	}
	// we don't copy the fields of `code` and `state` here, because they are lazy loaded.
	// we don't copy the `created` eigher, because it true only when the object is nil, which means "it was created newly by the uncommited db".
	// we need to copy the fields `deleted`, because it identifies whether the object is deleted or not.
	return &state{
		addr:         obj.Address(),
		nonce:        obj.Nonce(),
		balance:      new(uint256.Int).Set(obj.Balance()),
		selfDestruct: obj.selfDestructed,
		deleted:      obj.deleted, // deleted is true when a "selfDestruct=true" object is finalized. more details can be found in the method Finalize() of StateDB
		codeSize:     -1,          // mark the code "unloaded"
		state:        make(map[common.Hash]common.Hash),
	}
}

type reads map[common.Address]*state

func (sts reads) recordOnce(addr common.Address, st *state) *state {
	if _, ok := sts[addr]; !ok {
		if st == nil {
			// this is a newly created object
			sts[addr] = &state{addr: addr, created: true, state: make(map[common.Hash]common.Hash), codeSize: -1}
		} else {
			sts[addr] = st
		}
	}
	return st
}

func (sts reads) recordKVOnce(addr common.Address, key, val common.Hash) {
	obj := sts[addr]
	if _, ok := obj.state[key]; !ok {
		obj.state[key] = val
	}
}

func (sts reads) recordCodeOnce(addr common.Address, codeHash common.Hash, code []byte) {
	st := sts[addr]
	// we can't check whether the code is loaded or not, by checking codeHash == nil or code == nil (maybe the code is empty)
	// so we need another fields to record the code previous state.
	// and that is the `codeSize`, when codeSize == -1, it means the code is unloaded.
	if !st.codeIsLoaded() {
		st.codeHash = codeHash[:]
		st.code = code
		st.codeSize = len(code)
	}
}

func (sts reads) conflictsTo(maindb *StateDB) error {
	for addr, st := range sts {
		if err := st.conflicts(maindb); err != nil {
			return fmt.Errorf("conflict at address %s: %s", addr.String(), err.Error())
		}
	}
	return nil
}

// ===============================================
// Writes Methods
// it is used to record all the writes of the uncommitted db.

type writes map[common.Address]*state

const (
	ModifyNonce = 1 << iota
	ModifyBalance
	ModifyCode
	ModifyState
	ModifySelfDestruct
	ModifySelfDestruct6780
)

func (wst writes) setBalance(addr common.Address, balance *uint256.Int) {
	wst[addr].balance = balance
	wst[addr].modified |= ModifyBalance
}

func (wst writes) setNonce(addr common.Address, nonce uint64) {
	wst[addr].nonce = nonce
	wst[addr].modified |= ModifyNonce
}

func (wst writes) setState(addr common.Address, key, val common.Hash) {
	wst[addr].state[key] = val
	wst[addr].modified |= ModifyState
}

func (wst writes) setCode(addr common.Address, code []byte) {
	codeHash := crypto.Keccak256Hash(code)
	wst[addr].code = code
	wst[addr].codeHash = codeHash[:]
	wst[addr].codeSize = len(code)
	wst[addr].modified |= ModifyCode
}

func (wst writes) create(addr common.Address) *state {
	wst[addr] = &state{
		addr:    addr,
		balance: uint256.NewInt(0),
		nonce:   0,
		//need to init code & codeHash
		code:     nil,
		codeHash: types.EmptyCodeHash.Bytes(),
		// mark the code "unloaded"?
		//codeSize: -1,
		//need to reset storage
		state:   make(map[common.Hash]common.Hash),
		created: true,
	}
	wst[addr].markAllModified()
	return wst[addr]
}

func (wst writes) selfDestruct(addr common.Address) {
	obj := wst[addr]
	if obj == nil {
		return
	}
	obj.modified |= ModifySelfDestruct
	obj.selfDestruct = true
	obj.balance = uint256.NewInt(0)
}

func (wst writes) merge(maindb *StateDB) {
	for _, st := range wst {
		st.merge(maindb, false)
	}
}

type ParallelStateDB struct {
	db         Database
	trie       Trie
	prefetcher *triePrefetcher
	noTrie     bool
	hasher     crypto.KeccakState
	hasherLock sync.Mutex
	snaps      *snapshot.Tree    // Nil if snapshot is not available
	snap       snapshot.Snapshot // Nil if snapshot is not available

	trieParallelLock        sync.Mutex   // for parallel mode of trie, mostly for get states/objects from trie, lock required to handle trie tracer.
	stateObjectDestructLock sync.RWMutex // for parallel mode, used in mainDB for mergeSlot and conflict check.
	pendingDirtyLock        sync.Mutex

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
	stateObjects              *StateObjectSyncMap
	stateObjectsPending       map[common.Address]struct{} // State objects finalized but not yet written to the trie
	stateObjectsDirty         map[common.Address]struct{} // State objects modified in the current execution
	journalDirty              sync.Map                    // State objects modified in the current execution
	stateObjectsDestruct      sync.Map                    // State objects destructed in the block along with its previous value
	stateObjectsDestructDirty sync.Map

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be
	// returned by StateDB.Commit. Notably, this error is also shared
	// by all cached state objects in case the database failure occurs
	// when accessing state of accounts.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund atomic.Uint64

	logSize atomic.Int32

	// Preimages occurred seen by VM in the scope of block.
	preimages sync.Map

	// Per-transaction access list
	accessList *parallelAccessList

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

	// Testing hooks
	onCommit func(states *triestate.Set) // Hook invoked when commit is performed
}

// NewParallel creates a new parallel statedb
func NewParallel(root common.Hash, db Database, snaps *snapshot.Tree) (*ParallelStateDB, error) {
	tr, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	sdb := &ParallelStateDB{
		db:                  db,
		trie:                tr,
		originalRoot:        root,
		snaps:               snaps,
		accessList:          newParallelAccessList(),
		hasher:              crypto.NewKeccakState(),
		accounts:            make(map[common.Hash][]byte),
		storages:            make(map[common.Hash]map[common.Hash][]byte),
		accountsOrigin:      make(map[common.Address][]byte),
		storagesOrigin:      make(map[common.Address]map[common.Hash][]byte),
		stateObjectsPending: make(map[common.Address]struct{}),
		stateObjectsDirty:   make(map[common.Address]struct{}),
		stateObjects:        &StateObjectSyncMap{},
	}
	if sdb.snaps != nil {
		sdb.snap = sdb.snaps.Snapshot(root)
	}
	_, sdb.noTrie = tr.(*trie.EmptyTrie)
	return sdb, nil
}

func (p *ParallelStateDB) getBaseStateDB() *StateDB {
	panic("ParallelStateDB not support get base stateDB")
}

func (p *ParallelStateDB) getStateObject(address common.Address) *stateObject {
	if obj := p.getDeletedStateObject(address); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

func (p *ParallelStateDB) storeStateObj(address common.Address, object *stateObject) {
	p.stateObjects.StoreStateObject(address, object)
}

func (p *ParallelStateDB) CreateAccount(address common.Address) {
	// no matter it is got from dirty, unconfirmed or main DB
	// if addr not exist, preBalance will be common.U2560, it is same as new(big.Int) which
	// is the value newObject(),
	newObj, prev := p.createObject(address)
	if prev != nil {
		newObj.setBalance(prev.Balance())
	}
}

func (p *ParallelStateDB) SubBalance(addr common.Address, amount *uint256.Int) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
		return
	}
}

func (p *ParallelStateDB) AddBalance(addr common.Address, amount *uint256.Int) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
		return
	}
}

func (p *ParallelStateDB) GetBalance(addr common.Address) *uint256.Int {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.U2560
}

func (p *ParallelStateDB) GetNonce(addr common.Address) uint64 {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}
	return 0
}

func (p *ParallelStateDB) SetNonce(addr common.Address, nonce uint64) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (p *ParallelStateDB) GetCodeHash(addr common.Address) common.Hash {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return common.BytesToHash(stateObject.CodeHash())
	}
	return common.Hash{}
}

func (p *ParallelStateDB) GetCode(addr common.Address) []byte {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Code()
	}
	return nil
}

func (p *ParallelStateDB) SetCode(addr common.Address, code []byte) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (p *ParallelStateDB) GetCodeSize(addr common.Address) int {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.CodeSize()
	}
	return 0
}

func (p *ParallelStateDB) AddRefund(gas uint64) {
	p.refund.Add(gas)
}

func (p *ParallelStateDB) SubRefund(gas uint64) {
	for {
		refund := p.refund.Load()
		if gas > refund {
			panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, p.refund))
		}
		swapped := p.refund.CompareAndSwap(refund, refund-gas)
		if swapped {
			return
		}
	}
}

func (p *ParallelStateDB) GetRefund() uint64 {
	return p.refund.Load()
}

func (p *ParallelStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetCommittedState(hash)
	}
	return common.Hash{}
}

func (p *ParallelStateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetState(hash)
	}
	return common.Hash{}
}

func (p *ParallelStateDB) SetState(addr common.Address, key common.Hash, value common.Hash) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

func (p *ParallelStateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	panic("ParallelStateDB not support get TransientState")
}

func (p *ParallelStateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	panic("ParallelStateDB not support set TransientState")
}

func (p *ParallelStateDB) SelfDestruct(addr common.Address) {
	stateObject := p.getStateObject(addr)
	if stateObject == nil {
		return
	}
	p.journalDirty.Store(addr, struct{}{})
	stateObject.markSelfdestructed()
	stateObject.setBalance(new(uint256.Int))
}

func (p *ParallelStateDB) HasSelfDestructed(addr common.Address) bool {
	stateObject := p.getStateObject(addr)
	if stateObject != nil {
		return stateObject.selfDestructed
	}
	return false
}

func (p *ParallelStateDB) Selfdestruct6780(addr common.Address) {
	stateObject := p.getStateObject(addr)
	if stateObject == nil {
		return
	}
	if stateObject.created {
		p.SelfDestruct(addr)
	}
}

func (p *ParallelStateDB) Exist(addr common.Address) bool {
	return p.getStateObject(addr) != nil
}

func (p *ParallelStateDB) Empty(addr common.Address) bool {
	so := p.getStateObject(addr)
	return so == nil || so.empty()
}

func (p *ParallelStateDB) AddressInAccessList(addr common.Address) bool {
	return p.accessList.ContainsAddress(addr)
}

func (p *ParallelStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	return p.accessList.Contains(addr, slot)
}

func (p *ParallelStateDB) AddAddressToAccessList(addr common.Address) {
	p.accessList.AddAddress(addr)
}

func (p *ParallelStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	p.accessList.AddSlot(addr, slot)
}

func (p *ParallelStateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	//do nothing because not support access list and transientStorage
}

func (p *ParallelStateDB) RevertToSnapshot(revid int) {
	panic("ParallelStateDB not support revert to snapshot")
}

func (p *ParallelStateDB) Snapshot() int {
	return 0
}

func (p *ParallelStateDB) AddLog(log *types.Log) {
	panic("ParallelStateDB not support add log,use AddLogWithTx")
}

func (p *ParallelStateDB) AddLogWithTx(log *types.Log, txhash common.Hash, txIndex uint) {
	log.TxHash = txhash
	log.TxIndex = txIndex
	log.Index = uint(p.logSize.Load())
	//logs, loaded := p.logs.LoadOrStore(txhash, []*types.Log{log})
	//if loaded {
	//	logs = append(logs.([]*types.Log), log)
	//	p.logs.Store(txhash, logs)
	//}
	p.logSize.Add(1)
}

func (p *ParallelStateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := p.preimages.Load(hash); !ok {
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		p.preimages.Store(hash, pi)
	}
}

func (p *ParallelStateDB) TxIndex() int {
	// because in parallel env, txIndex is always 0
	return 0
}

func (p *ParallelStateDB) BeforeTxTransition() {
	//do nothing
}

func (p *ParallelStateDB) FinaliseRWSet() error {
	//do nothing
	return nil
}

func (p *ParallelStateDB) getDeletedStateObject(addr common.Address) *stateObject {
	for {
		// Prefer live objects if any is available
		if obj, _ := p.getStateObjectFromStateObjects(addr); obj != nil {
			return obj
		}
		data, ok := p.getStateObjectFromSnapshotOrTrie(addr)
		if !ok {
			return nil
		}
		// Insert into the live set
		obj := newObject(p, true, addr, data)
		setSuccess := p.setStateObjectIfEmpty(obj)
		if setSuccess {
			return obj
		} else {
			continue
		}
	}
}

func (p *ParallelStateDB) getStateObjectFromStateObjects(addr common.Address) (*stateObject, bool) {
	return p.loadStateObj(addr)
}

func (p *ParallelStateDB) loadStateObj(addr common.Address) (*stateObject, bool) {
	ret, ok := p.stateObjects.LoadStateObject(addr)
	return ret, ok
}

func (p *ParallelStateDB) getStateObjectFromSnapshotOrTrie(addr common.Address) (data *types.StateAccount, ok bool) {
	// If no live objects are available, attempt to use snapshots
	if p.snap != nil {
		start := time.Now()
		// the s.hasher is not thread-safe, let's use a mutex to protect it
		p.hasherLock.Lock()
		hash := crypto.HashData(p.hasher, addr.Bytes())
		p.hasherLock.Unlock()
		acc, err := p.snap.Account(hash)
		if metrics.EnabledExpensive {
			p.SnapshotAccountReads += time.Since(start)
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
				data.CodeHash = types.EmptyCodeHash.Bytes()
			}
			if data.Root == (common.Hash{}) {
				data.Root = types.EmptyRootHash
			}
		}
	}

	// If snapshot unavailable or reading from it failed, load from the database
	if data == nil {
		p.trieParallelLock.Lock()
		defer p.trieParallelLock.Unlock()
		var trie Trie
		trie = p.trie

		start := time.Now()
		var err error
		data, err = trie.GetAccount(addr)
		if metrics.EnabledExpensive {
			p.AccountReads += time.Since(start)
		}
		if err != nil {
			p.setError(fmt.Errorf("getDeleteStateObject (%x) error: %w", addr.Bytes(), err))
			return nil, false
		}
		if data == nil {
			return nil, false
		}
	}

	return data, true
}

func (p *ParallelStateDB) setError(err error) {
	if p.dbErr == nil {
		p.dbErr = err
	}
}

func (p *ParallelStateDB) setStateObject(object *stateObject) {
	p.stateObjects.Store(object.address, object)
}

func (p *ParallelStateDB) prefetchAccount(address common.Address) {
	if p.prefetcher == nil {
		return
	}
	p.trieParallelLock.Lock()
	defer p.trieParallelLock.Unlock()
	p.prefetcher.prefetch(common.Hash{}, p.originalRoot, common.Address{}, [][]byte{common.CopyBytes(address[:])})
}

func (p *ParallelStateDB) createObject(addr common.Address) (newobj *stateObject, prev *stateObject) {
	prev = p.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!
	newobj = newObject(p, true, addr, nil)
	if prev == nil {
		//p.journal.append(createObjectChange{account: &addr})
		p.journalDirty.Store(addr, struct{}{})
	} else {
		// The original account should be marked as destructed and all cached
		// account and storage data should be cleared as well. Note, it must
		// be done here, otherwise the destruction event of "original account"
		// will be lost.
		p.stateObjectDestructLock.Lock()
		_, prevdestruct := p.getStateObjectsDestruct(prev.address)
		if !prevdestruct {
			p.setStateObjectsDestruct(prev.address, prev.origin)
		}
		p.stateObjectDestructLock.Unlock()
		// There may be some cached account/storage data already since IntermediateRoot
		// will be called for each transaction before byzantium fork which will always
		// cache the latest account/storage data.
		//prevAccount, ok := p.accountsOrigin.Load(prev.address)
		//p.journal.append(resetObjectChange{
		//	account:                &addr,
		//	prev:                   prev,
		//	prevdestruct:           prevdestruct,
		//	prevAccount:            s.accounts[prev.addrHash],
		//	prevStorage:            s.storages[prev.addrHash],
		//	prevAccountOriginExist: ok,
		//	prevAccountOrigin:      prevAccount,
		//	prevStorageOrigin:      s.storagesOrigin[prev.address],
		//})
		p.journalDirty.Store(addr, struct{}{})
		p.AccountMux.Lock()
		delete(p.accounts, prev.addrHash)
		delete(p.accountsOrigin, prev.address)
		p.AccountMux.Unlock()
		p.StorageMux.Lock()
		delete(p.storages, prev.addrHash)
		delete(p.storagesOrigin, prev.address)
		p.StorageMux.Unlock()
	}

	newobj.created = true
	p.setStateObject(newobj)
	if prev != nil && !prev.deleted {
		return newobj, prev
	}
	return newobj, nil
}

func (p *ParallelStateDB) getStateObjectsDestruct(addr common.Address) (*types.StateAccount, bool) {
	if acc, ok := p.stateObjectsDestructDirty.Load(addr); ok {
		return acc.(*types.StateAccount), ok
	}
	acc, ok := p.stateObjectsDestruct.Load(addr)
	if ok {
		return acc.(*types.StateAccount), ok
	}
	return nil, ok
}

func (p *ParallelStateDB) SetExpectedStateRoot(root common.Hash) {
	p.expectedRoot = root
}

func (p *ParallelStateDB) setStateObjectsDestruct(address common.Address, acc *types.StateAccount) {
	p.stateObjectsDestructDirty.Store(address, acc)
}

func (p *ParallelStateDB) getOrNewStateObject(addr common.Address) *stateObject {
	stateObject := p.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = p.createObject(addr)
	}
	return stateObject
}

func (p *ParallelStateDB) StopPrefetcher() {
	if p.noTrie {
		return
	}

	if p.prefetcher != nil {
		p.prefetcher.close()
		p.prefetcher = nil
	}
}

func (p *ParallelStateDB) StartPrefetcher(namespace string) {
	if p.noTrie {
		return
	}

	if p.prefetcher != nil {
		p.prefetcher.close()
		p.prefetcher = nil
	}
	if p.snap != nil {
		p.prefetcher = newTriePrefetcher(p.db, p.originalRoot, namespace)
	}
}

func (p *ParallelStateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	p.Finalise(deleteEmptyObjects)
	p.AccountsIntermediateRoot()
	return p.StateIntermediateRoot()
}

func (p *ParallelStateDB) Error() error {
	return p.dbErr
}

func (p *ParallelStateDB) Timers() *Timers {
	return &Timers{
		AccountReads:         p.AccountReads,
		AccountHashes:        p.AccountHashes,
		AccountUpdates:       p.AccountUpdates,
		AccountCommits:       p.AccountCommits,
		StorageReads:         p.StorageReads,
		StorageHashes:        p.StorageHashes,
		StorageUpdates:       p.StorageUpdates,
		StorageCommits:       p.StorageCommits,
		SnapshotAccountReads: p.SnapshotAccountReads,
		SnapshotStorageReads: p.SnapshotStorageReads,
		SnapshotCommits:      p.SnapshotCommits,
		TrieDBCommits:        p.TrieDBCommits,
		TrieCommits:          p.TrieCommits,
		CodeCommits:          p.CodeCommits,
		TxDAGGenerate:        p.TxDAGGenerate,
	}
}

func (p *ParallelStateDB) Preimages() map[common.Hash][]byte {
	result := make(map[common.Hash][]byte)
	p.preimages.Range(func(key, value interface{}) bool {
		result[key.(common.Hash)] = value.([]byte)
		return true
	})
	return result
}

func (p *ParallelStateDB) Commit(block uint64, deleteEmptyObjects bool) (common.Hash, error) {
	// Short circuit in case any database failure occurred earlier.
	if p.dbErr != nil {
		p.StopPrefetcher()
		return common.Hash{}, fmt.Errorf("commit aborted due to earlier error: %v", p.dbErr)
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

	if !p.fullProcessed {
		p.stateRoot = p.IntermediateRoot(deleteEmptyObjects)
	}

	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			p.AccountCommits += time.Since(start)
			accountUpdatedMeter.Mark(int64(p.AccountUpdated))
			storageUpdatedMeter.Mark(int64(p.StorageUpdated))
			accountDeletedMeter.Mark(int64(p.AccountDeleted))
			storageDeletedMeter.Mark(int64(p.StorageDeleted))
			accountTrieUpdatedMeter.Mark(int64(accountTrieNodesUpdated))
			accountTrieDeletedMeter.Mark(int64(accountTrieNodesDeleted))
			//storageTriesUpdatedMeter.Mark(int64(storageTrieNodesUpdated))
			//storageTriesDeletedMeter.Mark(int64(storageTrieNodesDeleted))
			p.AccountUpdated, p.AccountDeleted = 0, 0
			p.StorageUpdated, p.StorageDeleted = 0, 0
		}(time.Now())
	}

	commitFuncs := []func() error{
		func() error {
			if metrics.EnabledExpensive {
				defer func(start time.Time) { p.TrieCommits += time.Since(start) }(time.Now())
			}
			if p.fullProcessed {
				if p.stateRoot = p.StateIntermediateRoot(); p.expectedRoot != p.stateRoot {
					log.Error("Invalid merkle root", "remote", p.expectedRoot, "local", p.stateRoot)
					return fmt.Errorf("invalid merkle root (remote: %x local: %x)", p.expectedRoot, p.stateRoot)
				}
			}
			var err error
			// Handle all state deletions first
			incomplete, err = p.handleDestruction(nodes)
			if err != nil {
				return err
			}

			tasks := make(chan func())
			type taskResult struct {
				err     error
				nodeSet *trienode.NodeSet
			}
			taskResults := make(chan taskResult, len(p.stateObjectsDirty))
			tasksNum := 0
			finishCh := make(chan struct{})

			threads := gopool.Threads(len(p.stateObjectsDirty))
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
			for addr := range p.stateObjectsDirty {
				if obj, _ := p.getStateObjectFromStateObjects(addr); !obj.deleted {
					tasks <- func() {
						// Write any storage changes in the state object to its storage trie
						if !p.noTrie {
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

			if !p.noTrie {
				var start time.Time
				if metrics.EnabledExpensive {
					start = time.Now()
				}
				root, set, err := p.trie.Commit(true)
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
					p.AccountCommits += time.Since(start)
				}

				origin := p.originalRoot
				if origin == (common.Hash{}) {
					origin = types.EmptyRootHash
				}

				if root != origin {
					start := time.Now()
					set := triestate.New(p.accountsOrigin, p.storagesOrigin, incomplete)
					if err := p.db.TrieDB().Update(root, origin, block, nodes, set); err != nil {
						return err
					}
					p.originalRoot = root
					if metrics.EnabledExpensive {
						p.TrieDBCommits += time.Since(start)
					}
					if p.onCommit != nil {
						p.onCommit(set)
					}
				}
			}
			wg.Wait()
			return nil
		},
		func() error {
			if metrics.EnabledExpensive {
				defer func(start time.Time) { p.CodeCommits += time.Since(start) }(time.Now())
			}
			codeWriter := p.db.DiskDB().NewBatch()
			for addr := range p.stateObjectsDirty {
				if obj, _ := p.getStateObjectFromStateObjects(addr); !obj.deleted {
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
			if p.snap != nil {
				if metrics.EnabledExpensive {
					defer func(start time.Time) { p.SnapshotCommits += time.Since(start) }(time.Now())
				}
				// Only update if there's a state transition (skip empty Clique blocks)
				if parent := p.snap.Root(); parent != p.expectedRoot {
					err := p.snaps.Update(p.expectedRoot, parent, p.convertAccountSet(&p.stateObjectsDestruct), p.accounts, p.storages)

					if err != nil {
						log.Warn("Failed to update snapshot tree", "from", parent, "to", p.expectedRoot, "err", err)
					}

					// Keep n diff layers in the memory
					// - head layer is paired with HEAD state
					// - head-1 layer is paired with HEAD-1 state
					// - head-(n-1) layer(bottom-most diff layer) is paired with HEAD-(n-1)state
					go func() {
						if err := p.snaps.Cap(p.expectedRoot, 128); err != nil {
							log.Warn("Failed to cap snapshot tree", "root", p.expectedRoot, "layers", 128, "err", err)
						}
					}()
				}
			}
			return nil
		},
	}
	defer p.StopPrefetcher()
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

	root := p.stateRoot
	p.snap = nil
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	// Clear all internal flags at the end of commit operation.
	p.accounts = make(map[common.Hash][]byte)
	p.storages = make(map[common.Hash]map[common.Hash][]byte)
	p.accountsOrigin = make(map[common.Address][]byte)
	p.storagesOrigin = make(map[common.Address]map[common.Hash][]byte)
	p.stateObjectsDirty = make(map[common.Address]struct{})
	p.stateObjectsDestruct = sync.Map{}
	return root, nil
}

func (p *ParallelStateDB) SetBalance(addr common.Address, amount *uint256.Int) {
	stateObject := p.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (p *ParallelStateDB) PrepareForParallel() {
	//do nothing
}

var goMaxProcs = runtime.GOMAXPROCS(0)

func (p *ParallelStateDB) Finalise(deleteEmptyObjects bool) {

	// finalise stateObjectsDestruct
	// The finalise of stateDB is called at verify & commit phase, which is global, no need to acquire the lock.
	p.stateObjectsDestructDirty.Range(func(key, value interface{}) bool {
		p.stateObjectsDestruct.Store(key, value)
		return true
	})
	p.stateObjectsDestructDirty = sync.Map{}

	runnerCount := goMaxProcs * 3 / 4
	dirtyChan := make(chan common.Address, runnerCount)
	addressesToPrefetch := make([][]byte, 0, 16)
	go func() {
		p.journalDirty.Range(func(key, value interface{}) bool {
			address := key.(common.Address)
			dirtyChan <- address
			addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(address[:]))
			return true
		})
		close(dirtyChan)
	}()
	var wg sync.WaitGroup
	wg.Add(runnerCount)
	for i := 0; i < runnerCount; i++ {
		err := gopool.Submit(func() {
			defer wg.Done()
			for {
				addr, isOpen := <-dirtyChan
				if !isOpen {
					return
				}
				obj, exist := p.getStateObjectFromStateObjects(addr)
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

					if _, ok := p.stateObjectsDestruct.Load(obj.address); !ok {
						p.stateObjectsDestruct.Store(obj.address, obj.origin)
					}
					// Note, we can't do this only at the end of a block because multiple
					// transactions within the same block might self destruct and then
					// resurrect an account; but the snapshotter needs both events.
					p.AccountMux.Lock()
					delete(p.accounts, obj.addrHash)
					delete(p.accountsOrigin, obj.address)
					p.AccountMux.Unlock()
					p.StorageMux.Lock()
					delete(p.storages, obj.addrHash)
					delete(p.storagesOrigin, obj.address)
					p.StorageMux.Unlock()
				} else {
					obj.finalise(true) // Prefetch slots in the background
				}

				obj.created = false
				p.pendingDirtyLock.Lock()
				p.stateObjectsPending[addr] = struct{}{}
				p.stateObjectsDirty[addr] = struct{}{}
				p.pendingDirtyLock.Unlock()
			}
		})
		if err != nil {
			panic(fmt.Errorf("parallel stateDB Finalise submit err:%w", err))
		}
	}
	wg.Wait()
	if p.prefetcher != nil && len(addressesToPrefetch) > 0 {
		p.trieParallelLock.Lock()
		p.prefetcher.prefetch(common.Hash{}, p.originalRoot, common.Address{}, addressesToPrefetch)
		p.trieParallelLock.Unlock()
	}
	// Invalidate journal because reverting across transactions is not allowed.
	p.clearJournalAndRefund()
}

func (p *ParallelStateDB) MarkFullProcessed() {
	p.fullProcessed = true
}

func (p *ParallelStateDB) AccessListCopy() *accessList {
	return p.accessList.Copy()
}

func (p *ParallelStateDB) SetTxContext(hash common.Hash, index int) {
	//do nothing
}

func (p *ParallelStateDB) SetAccessList(list *accessList) {
	//Do nothing, because after the Berlin hardfork,
	//the accessList will be cleared before the next transaction execution,
	//so there is no need for us to set the accessList of the underlying statedb.
}

func (p *ParallelStateDB) AccountsIntermediateRoot() {
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
	for addr := range p.stateObjectsPending {
		if obj, _ := p.getStateObjectFromStateObjects(addr); !obj.deleted {
			wg.Add(1)
			tasks <- func() {
				defer wg.Done()
				obj.updateRoot()

				// Cache the data until commit. Note, this update mechanism is not symmetric
				// to the deletion, because whereas it is enough to track account updates
				// at commit time, deletions need tracking at transaction boundary level to
				// ensure we capture state clearing.
				p.AccountMux.Lock()
				p.accounts[obj.addrHash] = types.SlimAccountRLP(obj.data)
				p.AccountMux.Unlock()
			}
		}
	}
	wg.Wait()
}

func (p *ParallelStateDB) StateIntermediateRoot() common.Hash {
	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	prefetcher := p.prefetcher
	r := p.originalRoot
	if p.prefetcher != nil {
		defer func() {
			p.prefetcher.close()
			p.prefetcher = nil
		}()
	}

	if prefetcher != nil {
		if prefetchTrie := prefetcher.trie(common.Hash{}, r); prefetchTrie != nil {
			p.trie = prefetchTrie
		}
	}

	if p.trie == nil {
		tr, err := p.db.OpenTrie(p.originalRoot)
		if err != nil {
			panic(fmt.Sprintf("failed to open trie tree %s", p.originalRoot))
		}
		p.trie = tr
	}

	usedAddrs := make([][]byte, 0, len(p.stateObjectsPending))

	for addr := range p.stateObjectsPending {
		if obj, _ := p.getStateObjectFromStateObjects(addr); obj.deleted {
			p.deleteStateObject(obj)
			p.AccountDeleted += 1
		} else {
			p.updateStateObject(obj)
			p.AccountUpdated += 1
		}
		usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
	}

	if prefetcher != nil {
		prefetcher.used(common.Hash{}, p.originalRoot, usedAddrs)
	}
	// parallel slotDB trie will be updated to mainDB since intermediateRoot happens after conflict check.
	// so it should be save to clear pending here.
	// otherwise there can be a case that the deleted object get ignored and processes as live object in verify phase.
	if len(p.stateObjectsPending) > 0 {
		p.stateObjectsPending = make(map[common.Address]struct{})
	}

	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { p.AccountHashes += time.Since(start) }(time.Now())
	}

	if p.noTrie {
		return p.expectedRoot
	} else {
		return p.trie.Hash()
	}
}

func (p *ParallelStateDB) GetPreimage(hash common.Hash) ([]byte, bool) {
	value, ok := p.preimages.Load(hash)
	if ok {
		return value.([]byte), ok
	}
	return nil, ok
}

func (p *ParallelStateDB) handleDestruction(nodes *trienode.MergedNodeSet) (map[common.Address]struct{}, error) {
	// Short circuit if geth is running with hash mode. This procedure can consume
	// considerable time and storage deletion isn't supported in hash mode, thus
	// preemptively avoiding unnecessary expenses.
	incomplete := make(map[common.Address]struct{})
	if p.db.TrieDB().Scheme() == rawdb.HashScheme {
		return incomplete, nil
	}
	var resultErr error

	p.stateObjectsDestruct.Range(func(key, value interface{}) bool {
		addr := key.(common.Address)
		prev := value.(*types.StateAccount)
		// The original account was non-existing, and it's marked as destructed
		// in the scope of block. It can be case (a) or (b).
		// - for (a), skip it without doing anything.
		// - for (b), track account's original value as nil. It may overwrite
		//   the data cached in s.accountsOrigin set by 'updateStateObject'.
		addrHash := crypto.Keccak256Hash(addr[:])
		if prev == nil {
			if _, ok := p.accounts[addrHash]; ok {
				p.accountsOrigin[addr] = nil // case (b)
			}
			return true
		}
		// It can overwrite the data in s.accountsOrigin set by 'updateStateObject'.
		p.accountsOrigin[addr] = types.SlimAccountRLP(*prev) // case (c) or (d)

		// Short circuit if the storage was empty.
		if prev.Root == types.EmptyRootHash {
			return true
		}

		// Remove storage slots belong to the account.
		aborted, slots, set, err := p.deleteStorage(addr, addrHash, prev.Root)
		if err != nil {
			resultErr = fmt.Errorf("failed to delete storage, err: %w", err)
			return false
		}
		// The storage is too huge to handle, skip it but mark as incomplete.
		// For case (d), the account is resurrected might with a few slots
		// created. In this case, wipe the entire storage state diff because
		// of aborted deletion.
		if aborted {
			incomplete[addr] = struct{}{}
			delete(p.storagesOrigin, addr)
			return true
		}
		if storages, ok := p.storagesOrigin[addr]; !ok || storages == nil {
			p.storagesOrigin[addr] = slots
		} else {
			// It can overwrite the data in s.storagesOrigin[addrHash] set by
			// 'object.updateTrie'.
			for key, val := range slots {
				p.storagesOrigin[addr][key] = val
			}
		}
		if err := nodes.Merge(set); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	return incomplete, nil
}

func (p *ParallelStateDB) deleteStorage(addr common.Address, addrHash common.Hash, root common.Hash) (bool, map[common.Hash][]byte, *trienode.NodeSet, error) {
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
	if p.snap != nil {
		aborted, size, slots, nodes, err = p.fastDeleteStorage(addrHash, root)
	}
	if p.snap == nil || err != nil {
		aborted, size, slots, nodes, err = p.slowDeleteStorage(addr, addrHash, root)
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

func (p *ParallelStateDB) fastDeleteStorage(addrHash common.Hash, root common.Hash) (bool, common.StorageSize, map[common.Hash][]byte, *trienode.NodeSet, error) {
	iter, err := p.snaps.StorageIterator(p.originalRoot, addrHash, common.Hash{})
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

func (p *ParallelStateDB) slowDeleteStorage(addr common.Address, addrHash common.Hash, root common.Hash) (bool, common.StorageSize, map[common.Hash][]byte, *trienode.NodeSet, error) {
	tr, err := p.db.OpenStorageTrie(p.originalRoot, addr, root, p.trie)
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

func (p *ParallelStateDB) convertAccountSet(set *sync.Map) map[common.Hash]struct{} {
	ret := make(map[common.Hash]struct{})
	set.Range(func(k, v interface{}) bool {
		addr := k.(common.Address)
		obj, exist := p.getStateObjectFromStateObjects(addr)
		if !exist {
			ret[crypto.Keccak256Hash(addr[:])] = struct{}{}
		} else {
			ret[obj.addrHash] = struct{}{}
		}
		return true
	})
	return ret
}

func (p *ParallelStateDB) clearJournalAndRefund() {
	p.journalDirty = sync.Map{}
	p.refund.Store(0)
}

func (p *ParallelStateDB) deleteStateObject(obj *stateObject) {
	if p.noTrie {
		return
	}

	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { p.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()
	if err := p.trie.DeleteAccount(addr); err != nil {
		p.setError(fmt.Errorf("deleteStateObject (%x) error: %v", addr[:], err))
	}
}

func (p *ParallelStateDB) updateStateObject(obj *stateObject) {
	if !p.noTrie {
		// Track the amount of time wasted on updating the account from the trie
		if metrics.EnabledExpensive {
			defer func(start time.Time) { p.AccountUpdates += time.Since(start) }(time.Now())
		}
		// Encode the account and update the account trie
		addr := obj.Address()
		p.trieParallelLock.Lock()
		if err := p.trie.UpdateAccount(addr, &obj.data); err != nil {
			p.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
		}
		if obj.dirtyCode {
			p.trie.UpdateContractCode(obj.Address(), common.BytesToHash(obj.CodeHash()), obj.code)
		}
		p.trieParallelLock.Unlock()
	}

	p.AccountMux.Lock()
	defer p.AccountMux.Unlock()
	// Cache the data until commit. Note, this update mechanism is not symmetric
	// to the deletion, because whereas it is enough to track account updates
	// at commit time, deletions need tracking at transaction boundary level to
	// ensure we capture state clearing.
	p.accounts[obj.addrHash] = types.SlimAccountRLP(obj.data)

	// Track the original value of mutated account, nil means it was not present.
	// Skip if it has been tracked (because updateStateObject may be called
	// multiple times in a block).
	if _, ok := p.accountsOrigin[obj.address]; !ok {
		if obj.origin == nil {
			p.accountsOrigin[obj.address] = nil
		} else {
			p.accountsOrigin[obj.address] = types.SlimAccountRLP(*obj.origin)
		}
	}
}

func (p *ParallelStateDB) appendJournal(journalEntry journalEntry) {
	if journalEntry.dirtied() != nil {
		p.journalDirty.Store(*journalEntry.dirtied(), struct{}{})
	}
}

func (p *ParallelStateDB) addJournalDirty(address common.Address) {
	p.journalDirty.Store(address, struct{}{})
}

func (p *ParallelStateDB) getPrefetcher() *triePrefetcher {
	return p.prefetcher
}

func (p *ParallelStateDB) getDB() Database {
	return p.db
}

func (p *ParallelStateDB) getOriginalRoot() common.Hash {
	return p.originalRoot
}

func (p *ParallelStateDB) getTrie() Trie {
	return p.trie
}

func (p *ParallelStateDB) getStateObjectDestructLock() *sync.RWMutex {
	return &p.stateObjectDestructLock
}

func (p *ParallelStateDB) getSnap() snapshot.Snapshot {
	return p.snap
}

func (p *ParallelStateDB) timeAddSnapshotStorageReads(du time.Duration) {
	p.SnapshotStorageReads += du
}

func (p *ParallelStateDB) getTrieParallelLock() *sync.Mutex {
	return &p.trieParallelLock
}

func (p *ParallelStateDB) timeAddStorageReads(du time.Duration) {
	p.StorageReads += du
}

func (p *ParallelStateDB) timeAddStorageUpdates(du time.Duration) {
	p.StorageUpdates += du
}

func (p *ParallelStateDB) countAddStorageDeleted(diff int) {
	p.StorageDeleted += diff
}

func (p *ParallelStateDB) countAddStorageUpdated(diff int) {
	p.StorageUpdated += diff
}

func (p *ParallelStateDB) getStorageMux() *sync.Mutex {
	return &p.StorageMux
}

func (p *ParallelStateDB) getStorages(hash common.Hash) map[common.Hash][]byte {
	return p.storages[hash]
}

func (p *ParallelStateDB) setStorages(hash common.Hash, storage map[common.Hash][]byte) {
	p.storages[hash] = storage
}

func (p *ParallelStateDB) getStoragesOrigin(address common.Address) map[common.Hash][]byte {
	return p.storagesOrigin[address]
}

func (p *ParallelStateDB) setStoragesOrigin(address common.Address, origin map[common.Hash][]byte) {
	p.storagesOrigin[address] = origin
}

func (p *ParallelStateDB) timeAddStorageHashes(du time.Duration) {
	p.StorageHashes += du
}

func (p *ParallelStateDB) timeAddStorageCommits(du time.Duration) {
	p.StorageCommits += du
}

func (p *ParallelStateDB) setStateObjectIfEmpty(obj *stateObject) bool {
	_, loaded := p.stateObjects.LoadOrStore(obj.address, obj)
	return !loaded
}

func (s *ParallelStateDB) CheckFeeReceiversRWSet() {
	return
}
