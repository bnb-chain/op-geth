package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"golang.org/x/exp/slices"
	"strings"
	"sync"
)

const (
	AccountStatePrefix = 'a'
	StorageStatePrefix = 's'
)

type RWKey [1 + common.AddressLength + common.HashLength]byte

type AccountState byte

const (
	AccountSelf AccountState = iota
	AccountNonce
	AccountBalance
	AccountCodeHash
	AccountSuicide
)

func AccountStateKey(account common.Address, state AccountState) RWKey {
	var key RWKey
	key[0] = AccountStatePrefix
	copy(key[1:], account.Bytes())
	key[1+common.AddressLength] = byte(state)
	return key
}

func StorageStateKey(account common.Address, state common.Hash) RWKey {
	var key RWKey
	key[0] = StorageStatePrefix
	copy(key[1:], account.Bytes())
	copy(key[1+common.AddressLength:], state.Bytes())
	return key
}

func (key *RWKey) IsAccountState() (bool, AccountState) {
	return AccountStatePrefix == key[0], AccountState(key[1+common.AddressLength])
}

func (key *RWKey) IsAccountSelf() bool {
	ok, s := key.IsAccountState()
	if !ok {
		return false
	}
	return s == AccountSelf
}

func (key *RWKey) IsAccountSuicide() bool {
	ok, s := key.IsAccountState()
	if !ok {
		return false
	}
	return s == AccountSuicide
}

func (key *RWKey) ToAccountSelf() RWKey {
	return AccountStateKey(key.Addr(), AccountSelf)
}

func (key *RWKey) IsStorageState() bool {
	return StorageStatePrefix == key[0]
}

func (key *RWKey) String() string {
	return hex.EncodeToString(key[:])
}

func (key *RWKey) Addr() common.Address {
	return common.BytesToAddress(key[1 : 1+common.AddressLength])
}

// StateVersion record specific TxIndex & TxIncarnation
// if TxIndex equals to -1, it means the state read from DB.
type StateVersion struct {
	TxIndex int
	// TODO(galaio): used for multi ver state
	TxIncarnation int
}

// ReadRecord keep read value & its version
type ReadRecord struct {
	StateVersion
	Val interface{}
}

// WriteRecord keep latest state value & change count
type WriteRecord struct {
	Val interface{}
}

// RWSet record all read & write set in txs
// Attention: this is not a concurrent safety structure
type RWSet struct {
	ver      StateVersion
	readSet  map[RWKey]*ReadRecord
	writeSet map[RWKey]*WriteRecord

	// some flags
	mustSerial bool
}

func NewRWSet(ver StateVersion) *RWSet {
	return &RWSet{
		ver:      ver,
		readSet:  make(map[RWKey]*ReadRecord),
		writeSet: make(map[RWKey]*WriteRecord),
	}
}

func (s *RWSet) RecordRead(key RWKey, ver StateVersion, val interface{}) {
	// only record the first read version
	if _, exist := s.readSet[key]; exist {
		return
	}
	s.readSet[key] = &ReadRecord{
		StateVersion: ver,
		Val:          val,
	}
}

func (s *RWSet) RecordWrite(key RWKey, val interface{}) {
	wr, exist := s.writeSet[key]
	if !exist {
		s.writeSet[key] = &WriteRecord{
			Val: val,
		}
		return
	}
	wr.Val = val
}

func (s *RWSet) Version() StateVersion {
	return s.ver
}

func (s *RWSet) ReadSet() map[RWKey]*ReadRecord {
	return s.readSet
}

func (s *RWSet) WriteSet() map[RWKey]*WriteRecord {
	return s.writeSet
}

func (s *RWSet) WithSerialFlag() *RWSet {
	s.mustSerial = true
	return s
}

func (s *RWSet) String() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("tx: %v, inc: %v\nreadSet: [", s.ver.TxIndex, s.ver.TxIncarnation))
	i := 0
	for key, _ := range s.readSet {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v", key.String()))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v", key.String()))
		i++
	}
	builder.WriteString("]\nwriteSet: [")
	i = 0
	for key, _ := range s.writeSet {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v", key.String()))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v", key.String()))
		i++
	}
	builder.WriteString("]\n")
	return builder.String()
}

// isEqualRWVal compare state
func isEqualRWVal(key RWKey, src interface{}, compared interface{}) bool {
	if ok, state := key.IsAccountState(); ok {
		switch state {
		case AccountBalance:
			if src != nil && compared != nil {
				return equalUint256(src.(*uint256.Int), compared.(*uint256.Int))
			}
			return src == compared
		case AccountNonce:
			return src.(uint64) == compared.(uint64)
		case AccountCodeHash:
			if src != nil && compared != nil {
				return slices.Equal(src.([]byte), compared.([]byte))
			}
			return src == compared
		}
		return false
	}

	if src != nil && compared != nil {
		return src.(common.Hash) == compared.(common.Hash)
	}
	return src == compared
}

func equalUint256(s, c *uint256.Int) bool {
	if s != nil && c != nil {
		return s.Eq(c)
	}

	return s == c
}

type PendingWrite struct {
	Ver StateVersion
	Val interface{}
}

func NewPendingWrite(ver StateVersion, wr *WriteRecord) *PendingWrite {
	return &PendingWrite{
		Ver: ver,
		Val: wr.Val,
	}
}

func (w *PendingWrite) TxIndex() int {
	return w.Ver.TxIndex
}

func (w *PendingWrite) TxIncarnation() int {
	return w.Ver.TxIncarnation
}

type PendingWrites struct {
	list []*PendingWrite
}

func NewPendingWrites() *PendingWrites {
	return &PendingWrites{
		list: make([]*PendingWrite, 0),
	}
}

func (w *PendingWrites) Append(pw *PendingWrite) {
	if i, found := w.SearchTxIndex(pw.TxIndex()); found {
		w.list[i] = pw
		return
	}

	w.list = append(w.list, pw)
	for i := len(w.list) - 1; i > 0; i-- {
		if w.list[i].TxIndex() > w.list[i-1].TxIndex() {
			break
		}
		w.list[i-1], w.list[i] = w.list[i], w.list[i-1]
	}
}

func (w *PendingWrites) SearchTxIndex(txIndex int) (int, bool) {
	n := len(w.list)
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		// i ≤ h < j
		if w.list[h].TxIndex() < txIndex {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < n && w.list[i].TxIndex() == txIndex
}

func (w *PendingWrites) FindLastWrite(txIndex int) *PendingWrite {
	var i, _ = w.SearchTxIndex(txIndex)
	for j := i - 1; j >= 0; j-- {
		if w.list[j].TxIndex() < txIndex {
			return w.list[j]
		}
	}

	return nil
}

type MVStates struct {
	rwSets          map[int]*RWSet
	pendingWriteSet map[RWKey]*PendingWrites

	// dependency map cache for generating TxDAG
	// depsCache[i].exist(j) means j->i, and i > j
	depsCache map[int]TxDepMap

	// execution stat infos
	stats map[int]*ExeStat
	lock  sync.RWMutex
}

func NewMVStates(txCount int) *MVStates {
	return &MVStates{
		rwSets:          make(map[int]*RWSet, txCount),
		pendingWriteSet: make(map[RWKey]*PendingWrites, txCount*8),
		depsCache:       make(map[int]TxDepMap, txCount),
		stats:           make(map[int]*ExeStat, txCount),
	}
}

func (s *MVStates) RWSets() map[int]*RWSet {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.rwSets
}

func (s *MVStates) Stats() map[int]*ExeStat {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.stats
}

func (s *MVStates) RWSet(index int) *RWSet {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if index >= len(s.rwSets) {
		return nil
	}
	return s.rwSets[index]
}

// ReadState TODO(galaio): read state from MVStates
func (s *MVStates) ReadState(key RWKey) (interface{}, bool) {
	return nil, false
}

// FulfillRWSet it can execute as async, and rwSet & stat must guarantee read-only
// TODO(galaio): try to generate TxDAG, when fulfill RWSet
// TODO(galaio): support flag to stat execution as optional
func (s *MVStates) FulfillRWSet(rwSet *RWSet, stat *ExeStat) error {
	log.Debug("FulfillRWSet", "s.len", len(s.rwSets), "cur", rwSet.ver.TxIndex, "reads", len(rwSet.readSet), "writes", len(rwSet.writeSet))
	s.lock.Lock()
	defer s.lock.Unlock()
	index := rwSet.ver.TxIndex
	if s := s.rwSets[index]; s != nil {
		return errors.New("refill a exist RWSet")
	}
	if stat != nil {
		if stat.txIndex != index {
			return errors.New("wrong execution stat")
		}
		s.stats[index] = stat
	}

	s.resolveDepsCache(index, rwSet)
	// append to pending write set
	for k, v := range rwSet.writeSet {
		// TODO(galaio): this action is only for testing, it can be removed in production mode.
		// ignore no changed write record
		checkRWSetInconsistent(index, k, rwSet.readSet, rwSet.writeSet)
		if _, exist := s.pendingWriteSet[k]; !exist {
			s.pendingWriteSet[k] = NewPendingWrites()
		}
		s.pendingWriteSet[k].Append(NewPendingWrite(rwSet.ver, v))
	}
	s.rwSets[index] = rwSet
	return nil
}

func (s *MVStates) resolveDepsCache(index int, rwSet *RWSet) {
	// analysis dep, if the previous transaction is not executed/validated, re-analysis is required
	if _, ok := s.depsCache[index]; !ok {
		s.depsCache[index] = NewTxDeps(0)
	}
	for prev := 0; prev < index; prev++ {
		// if there are some parallel execution or system txs, it will fulfill in advance
		// it's ok, and try re-generate later
		if _, ok := s.rwSets[prev]; !ok {
			continue
		}
		// TODO: check if there are RW with system address for gas delay calculation
		// check if there has written op before i
		if checkDependency(s.rwSets[prev].writeSet, rwSet.readSet) {
			s.depsCache[index].add(prev)
			// clear redundancy deps compared with prev
			for dep := range s.depsCache[index] {
				if s.depsCache[prev].exist(dep) {
					s.depsCache[index].remove(dep)
				}
			}
		}
	}
}

func checkRWSetInconsistent(index int, k RWKey, readSet map[RWKey]*ReadRecord, writeSet map[RWKey]*WriteRecord) bool {
	var (
		readOk  bool
		writeOk bool
		r       *WriteRecord
	)

	if k.IsAccountSuicide() {
		_, readOk = readSet[k.ToAccountSelf()]
	} else {
		_, readOk = readSet[k]
	}

	r, writeOk = writeSet[k]
	if readOk != writeOk {
		// check if it's correct? read nil, write non-nil
		log.Info("checkRWSetInconsistent find inconsistent", "tx", index, "k", k.String(), "read", readOk, "write", writeOk, "val", r.Val)
		return true
	}

	return false
}

// ResolveTxDAG generate TxDAG from RWSets
func (s *MVStates) ResolveTxDAG() TxDAG {
	rwSets := s.RWSets()
	txDAG := NewPlainTxDAG(len(rwSets))
	for i := len(rwSets) - 1; i >= 0; i-- {
		txDAG.TxDeps[i].TxIndexes = []uint64{}
		if rwSets[i].mustSerial {
			txDAG.TxDeps[i].Relation = 1
			continue
		}
		if s.depsCache[i] == nil {
			s.resolveDepsCache(i, rwSets[i])
		}
		txDAG.TxDeps[i].TxIndexes = s.depsCache[i].toArray()
	}

	return txDAG
}

func checkDependency(writeSet map[RWKey]*WriteRecord, readSet map[RWKey]*ReadRecord) bool {
	// check tx dependency, only check key, skip version
	for k, _ := range writeSet {
		// check suicide, add read address flag, it only for check suicide quickly, and cannot for other scenarios.
		if k.IsAccountSuicide() {
			if _, ok := readSet[k.ToAccountSelf()]; ok {
				return true
			}
			continue
		}
		if _, ok := readSet[k]; ok {
			return true
		}
	}

	return false
}

type TxDepMap map[int]struct{}

func NewTxDeps(cap int) TxDepMap {
	return make(map[int]struct{}, cap)
}

func (m TxDepMap) add(index int) {
	m[index] = struct{}{}
}

func (m TxDepMap) exist(index int) bool {
	_, ok := m[index]
	return ok
}

func (m TxDepMap) toArray() []uint64 {
	ret := make([]uint64, 0, len(m))
	for index := range m {
		ret = append(ret, uint64(index))
	}
	slices.Sort(ret)
	return ret
}

func (m TxDepMap) remove(index int) {
	delete(m, index)
}
