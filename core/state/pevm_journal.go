package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

type jBalance struct {
	created bool // whether the object is newly created in the uncommitted db
	addr    common.Address
	prev    *uint256.Int
}

func newJBalance(obj *state, addr common.Address) *jBalance {
	if obj == nil {
		return &jBalance{
			created: true,
			addr:    addr,
			prev:    nil,
		}
	} else {
		return &jBalance{
			created: false,
			addr:    addr,
			prev:    new(uint256.Int).Set(obj.balance),
		}
	}
}

func (j *jBalance) revert(db *UncommittedDB) {
	if j.created {
		delete(db.cache, j.addr)
		return
	}
	db.cache.setBalance(j.addr, j.prev)
}

type jNonce struct {
	created bool
	addr    common.Address
	nonce   uint64
}

func newJNonce(obj *state, addr common.Address) *jNonce {
	if obj == nil {
		return &jNonce{
			created: true,
			addr:    addr,
			nonce:   0,
		}
	} else {
		return &jNonce{
			created: false,
			addr:    addr,
			nonce:   obj.nonce,
		}
	}
}

func (j *jNonce) revert(db *UncommittedDB) {
	if j.created {
		delete(db.cache, j.addr)
		return
	}
	db.cache.setNonce(j.addr, j.nonce)
}

type jStorage struct {
	created    bool
	addr       common.Address
	keyCreated bool // whether the key is newly created in the object
	key, val   common.Hash
}

func newJStorage(obj *state, addr common.Address, key common.Hash) *jStorage {
	if obj == nil {
		return &jStorage{
			created:    true,
			addr:       addr,
			keyCreated: false,
			key:        key,
			val:        common.Hash{},
		}
	}
	val, ok := obj.state[key]
	if !ok {
		return &jStorage{
			created:    false,
			addr:       addr,
			keyCreated: true,
			key:        key,
			val:        common.Hash{},
		}
	}
	return &jStorage{
		created:    false,
		addr:       addr,
		keyCreated: false,
		key:        key,
		val:        val,
	}
}

func (j *jStorage) revert(db *UncommittedDB) {
	if j.created {
		delete(db.cache, j.addr)
		return
	}
	if j.keyCreated {
		delete(db.cache[j.addr].state, j.key)
	} else {
		db.cache.setState(j.addr, j.key, j.val)
	}
}

type jCode struct {
	created bool
	addr    common.Address
	code    []byte
}

func newJCode(obj *state, addr common.Address) *jCode {
	if obj == nil {
		return &jCode{
			created: true,
			addr:    addr,
			code:    nil,
		}
	} else {
		return &jCode{
			created: false,
			addr:    addr,
			code:    append([]byte(nil), obj.code...),
		}
	}
}

func (j *jCode) revert(db *UncommittedDB) {
	if j.created {
		delete(db.cache, j.addr)
		return
	}
	db.cache.setCode(j.addr, j.code)
}

type jCreateAccount struct {
	replaced bool
	addr     common.Address
	obj      *state
}

func newJCreateAccount(obj *state, addr common.Address) *jCreateAccount {
	if obj == nil {
		return &jCreateAccount{
			addr:     addr,
			obj:      nil,
			replaced: false,
		}
	} else {
		return &jCreateAccount{
			addr:     addr,
			obj:      obj.clone(),
			replaced: true,
		}
	}
}

func (j *jCreateAccount) revert(db *UncommittedDB) {
	if !j.replaced {
		delete(db.cache, j.addr)
	} else {
		db.cache[j.addr] = j.obj
	}
}

type jSelfDestruct struct {
	addr common.Address
	obj  *state
}

func newJSelfDestruct(obj *state) *jSelfDestruct {
	if obj == nil {
		//@TODO: should we handle this case?
		return &jSelfDestruct{
			addr: common.Address{},
			obj:  nil,
		}
	}
	return &jSelfDestruct{
		addr: obj.addr,
		obj: &state{
			modified: obj.modified,
			addr:     obj.addr,
			balance:  new(uint256.Int).Set(obj.balance),
			nonce:    obj.nonce,
			code:     append([]byte(nil), obj.code...),
			codeHash: append([]byte(nil), obj.codeHash...),
			codeSize: obj.codeSize,
			created:  obj.created,
			deleted:  obj.deleted,
		},
	}
}

func (j *jSelfDestruct) revert(db *UncommittedDB) {
	db.cache[j.addr] = j.obj
}

type jLogs struct {
}

func (j *jLogs) revert(db *UncommittedDB) {
	if len(db.logs) == 0 {
		// it should never happen
		return
	}
	db.logs = db.logs[:len(db.logs)-1]
}

type jRefund struct {
	prev uint64
}

func newJRefund(prev uint64) *jRefund {
	return &jRefund{
		prev: prev,
	}
}

func (j *jRefund) revert(db *UncommittedDB) {
	db.refund = j.prev
}

type jPreimage struct {
	created bool
	hash    common.Hash
}

func newJPreimage(hash common.Hash) *jPreimage {
	return &jPreimage{
		created: true,
		hash:    hash,
	}
}

func (j *jPreimage) revert(db *UncommittedDB) {
	delete(db.preimages, j.hash)
}

type jAccessList struct {
	addr *common.Address
}

func (j *jAccessList) revert(db *UncommittedDB) {
	db.accessList.DeleteAddress(*j.addr)
}

type jAccessListSlot struct {
	addr *common.Address
	slot *common.Hash
}

func (j *jAccessListSlot) revert(db *UncommittedDB) {
	db.accessList.DeleteSlot(*j.addr, *j.slot)
}

type jLog struct {
	i uint
}

func newJLog(i uint) *jLog {
	return &jLog{
		i: i,
	}
}

func (j *jLog) revert(db *UncommittedDB) {
	db.logs = db.logs[:j.i]
}

type jTransientStorage struct {
	addr     common.Address
	key, val common.Hash
}

func newJTransientStorage(addr common.Address, key, val common.Hash) *jTransientStorage {
	return &jTransientStorage{
		addr: addr,
		key:  key,
		val:  val,
	}
}

func (j *jTransientStorage) revert(db *UncommittedDB) {
	storage, ok := db.transientStorage[j.addr]
	if !ok {
		return
	}
	delete(storage, j.key)
	if len(storage) == 0 {
		delete(db.transientStorage, j.addr)
	}
}

type ujournal []ustate

func (j *ujournal) append(st ustate) {
	*j = append(*j, st)
}

func (j *ujournal) revertTo(db *UncommittedDB, snapshot int) {
	if snapshot < 0 || snapshot >= len(*j) {
		return // invalid snapshot index
	}
	for i := len(*j) - 1; i >= snapshot; i-- {
		(*j)[i].revert(db)
	}
	*j = (*j)[:snapshot]
}

func (j *ujournal) snapshot() int {
	return len(*j)
}

type ustate interface {
	revert(db *UncommittedDB)
}
