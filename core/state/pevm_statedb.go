package state

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
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

	maindb *StateDB
}

func NewUncommittedDB(maindb *StateDB) *UncommittedDB {
	return &UncommittedDB{
		accessList:       newAccessList(),
		transientStorage: newTransientStorage(),
		maindb:           maindb,
		reads:            make(reads),
		cache:            make(writes),
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
		pst.accessList = pst.maindb.accessList.Copy()
	}
	// Reset transient storage at the beginning of transaction execution
	pst.transientStorage = newTransientStorage()
}

// ===============================================
// Object Methods
//  1. journal
//  2. object

func (pst *UncommittedDB) SubBalance(addr common.Address, amount *uint256.Int) {
	pst.journal.append(newJBalance(pst.cache[addr], addr))
	obj := pst.getOrNewObject(addr)
	newb := new(uint256.Int).Sub(obj.balance, amount)
	pst.cache.setBalance(addr, newb)
}

func (pst *UncommittedDB) AddBalance(addr common.Address, amount *uint256.Int) {
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
		pst.cache.selfDestruct(addr)
	}
}

func (pst *UncommittedDB) Exist(addr common.Address) bool {
	return pst.getObject(addr) != nil
}

func (pst *UncommittedDB) Empty(addr common.Address) bool {
	obj := pst.getObject(addr)
	return obj == nil || obj.empty()
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
// (is it necessary to do snapshot and revert ?)
func (pst *UncommittedDB) RevertToSnapshot(id int) {
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
		for h, pi := range pst.maindb.preimages {
			pst.preimagesReads[h], pst.preimages[h] = pi, pi
		}
	}
	if _, ok := pst.preimagesReads[hash]; ok {
		return
	}
	// get and record the preimage state from the maindb
	preimageFromMaindb, ok := pst.maindb.preimages[hash]
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
	if err := pst.hasLogConflict(pst.maindb); err != nil {
		return err
	}
	if err := pst.hasRefundConflict(pst.maindb); err != nil {
		return err
	}
	if err := pst.hasPreimageConflict(pst.maindb); err != nil {
		return err
	}
	return pst.reads.conflictsTo(pst.maindb)
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
		pst.maindb.preimages[hash] = preimage
	}
	// 2. merge accesslist writes
	// we need to merge accessList before Berlin hardfork
	if !pst.isBerlin {
		for addr, slot := range pst.accessList.addresses {
			pst.maindb.accessList.AddAddress(addr)
			if slot >= 0 {
				slots := pst.accessList.slots[slot]
				for hash := range slots {
					pst.maindb.accessList.AddSlot(addr, hash)
				}
			}
		}
	} else {
		// after Berlin hardfork, all the accessList should be reset before a transaction was executed
		pst.maindb.accessList = pst.accessList
	}
	// 3. merge logs writes
	for _, st := range pst.cache {
		st.merge(pst.maindb)
	}
	// 4. merge object states
	for _, log := range pst.logs {
		pst.maindb.AddLog(log)
	}
	// 5. merge refund
	if pst.refund != 0 {
		pst.maindb.AddRefund(pst.refund)
	}
	// clean empty objects if needed
	for _, obj := range pst.cache {
		if obj.selfDestruct || (deleteEmptyObjects && obj.empty()) {
			obj.deleted = true
		}
		// we don't need to do obj.finalize() here, it will be done in the maindb.Finalize()
		// just mark the object as deleted
		obj.created = false
	}
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
func (pst *UncommittedDB) getDeletedObject(addr common.Address, maindb *StateDB) (o *state) {
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
func (pst *UncommittedDB) getDeletedObjectWithCode(addr common.Address, maindb *StateDB) (o *state) {
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
	if deletedObj == nil {
		pst.reads.recordCodeOnce(addr, common.Hash{}, nil)
	} else {
		pst.reads.recordCodeOnce(addr, common.BytesToHash(deletedObj.CodeHash()), deletedObj.Code())
	}
	// set code into the cache
	code, codeHash := deletedObj.Code(), common.BytesToHash(deletedObj.CodeHash())
	o.code = code
	o.codeHash = codeHash[:]
	o.codeSize = len(code)
	return o
}

// getDeletedObjectWithState return an object with state, and load the state from maindb if it is not loaded.
func (pst *UncommittedDB) getDeletedObjectWithState(addr common.Address, maindb *StateDB, hash common.Hash) (o *state) {
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

func (s state) merge(maindb *StateDB) {
	// 1. merge the balance
	// 2. merge the nonce
	// 3. merge the code
	// 4. merge the state
	if s.modified&ModifySelfDestruct != 0 {
		maindb.SelfDestruct(s.addr)
		return
	}
	obj := maindb.getOrNewStateObject(s.addr)
	if s.modified&ModifyBalance != 0 {
		obj.SetBalance(s.balance)
	}
	if s.modified&ModifyNonce != 0 {
		obj.SetNonce(s.nonce)
	}
	if s.modified&ModifyCode != 0 {
		obj.SetCode(common.BytesToHash(s.codeHash), s.code)
	}
	if s.modified&ModifyState != 0 {
		for key, val := range s.state {
			obj.SetState(key, val)
		}
		//TODO: should we reset all kv pairs if the s.state == nil ?
	}
}

func (s *state) empty() bool {
	return s.nonce == 0 && s.balance.Sign() == 0 && bytes.Equal(s.codeHash, types.EmptyCodeHash.Bytes())
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
		st.merge(maindb)
	}
}
