package types

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"golang.org/x/exp/slices"
)

type AccountState byte

var (
	AccountSelf     AccountState = 0x01
	AccountNonce    AccountState = 0x02
	AccountBalance  AccountState = 0x04
	AccountCodeHash AccountState = 0x08
	AccountSuicide  AccountState = 0x10
)

const (
	asyncDepGenChanSize = 1000
)

// StateVersion record specific TxIndex & TxIncarnation
// if TxIndex equals to -1, it means the state read from DB.
type StateVersion struct {
	TxIndex int
	// Tx incarnation used for multi ver state
	TxIncarnation int
}

// RWSet record all read & write set in txs
// Attention: this is not a concurrent safety structure
type RWSet struct {
	ver          StateVersion
	accReadSet   map[common.Address]map[AccountState]RWItem
	accWriteSet  map[common.Address]map[AccountState]RWItem
	slotReadSet  map[common.Address]map[common.Hash]RWItem
	slotWriteSet map[common.Address]map[common.Hash]RWItem

	// some flags
	rwRecordDone bool
	excludedTx   bool
}

func NewRWSet(ver StateVersion) *RWSet {
	return &RWSet{
		ver:          ver,
		accReadSet:   make(map[common.Address]map[AccountState]RWItem),
		accWriteSet:  make(map[common.Address]map[AccountState]RWItem),
		slotReadSet:  make(map[common.Address]map[common.Hash]RWItem),
		slotWriteSet: make(map[common.Address]map[common.Hash]RWItem),
	}
}

func (s *RWSet) RecordAccountRead(addr common.Address, state AccountState, ver StateVersion, val interface{}) {
	// only record the first read version
	sub, ok := s.accReadSet[addr]
	if !ok {
		s.accReadSet[addr] = make(map[AccountState]RWItem)
		s.accReadSet[addr][state] = RWItem{
			Ver: ver,
			Val: val,
		}
		return
	}
	if _, ok = sub[state]; ok {
		return
	}
	s.accReadSet[addr][state] = RWItem{
		Ver: ver,
		Val: val,
	}
}

func (s *RWSet) RecordStorageRead(addr common.Address, slot common.Hash, ver StateVersion, val interface{}) {
	// only record the first read version
	sub, ok := s.slotReadSet[addr]
	if !ok {
		s.slotReadSet[addr] = make(map[common.Hash]RWItem)
		s.slotReadSet[addr][slot] = RWItem{
			Ver: ver,
			Val: val,
		}
		return
	}
	if _, ok = sub[slot]; ok {
		return
	}
	s.slotReadSet[addr][slot] = RWItem{
		Ver: ver,
		Val: val,
	}
}

func (s *RWSet) RecordAccountWrite(addr common.Address, state AccountState, val interface{}) {
	_, ok := s.accWriteSet[addr]
	if !ok {
		s.accWriteSet[addr] = make(map[AccountState]RWItem)
	}
	s.accWriteSet[addr][state] = RWItem{
		Ver: s.ver,
		Val: val,
	}
}

func (s *RWSet) RecordStorageWrite(addr common.Address, slot common.Hash, val interface{}) {
	_, ok := s.slotWriteSet[addr]
	if !ok {
		s.slotWriteSet[addr] = make(map[common.Hash]RWItem)
	}
	s.slotWriteSet[addr][slot] = RWItem{
		Ver: s.ver,
		Val: val,
	}
}

func (s *RWSet) queryAccReadItem(addr common.Address, state AccountState) *RWItem {
	sub, ok := s.accReadSet[addr]
	if !ok {
		return nil
	}

	ret, ok := sub[state]
	if !ok {
		return nil
	}
	return &ret
}

func (s *RWSet) querySlotReadItem(addr common.Address, slot common.Hash) *RWItem {
	sub, ok := s.slotReadSet[addr]
	if !ok {
		return nil
	}

	ret, ok := sub[slot]
	if !ok {
		return nil
	}
	return &ret
}

func (s *RWSet) Version() StateVersion {
	return s.ver
}

func (s *RWSet) ReadSet() (map[common.Address]map[AccountState]RWItem, map[common.Address]map[common.Hash]RWItem) {
	return s.accReadSet, s.slotReadSet
}

func (s *RWSet) WriteSet() (map[common.Address]map[AccountState]RWItem, map[common.Address]map[common.Hash]RWItem) {
	return s.accWriteSet, s.slotWriteSet
}

func (s *RWSet) WithExcludedTxFlag() *RWSet {
	s.excludedTx = true
	return s
}

func (s *RWSet) String() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("tx: %v, inc: %v\nreadSet: [", s.ver.TxIndex, s.ver.TxIncarnation))
	i := 0
	for key, _ := range s.accReadSet {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v", key.String()))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v", key.String()))
		i++
	}
	for key, _ := range s.slotReadSet {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v", key.String()))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v", key.String()))
		i++
	}
	builder.WriteString("]\nwriteSet: [")
	i = 0
	for key, _ := range s.accWriteSet {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v", key.String()))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v", key.String()))
		i++
	}
	for key, _ := range s.slotWriteSet {
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
func isEqualRWVal(accState *AccountState, src interface{}, compared interface{}) bool {
	if accState != nil {
		switch *accState {
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

type RWItem struct {
	Ver StateVersion
	Val interface{}
}

func NewRWItem(ver StateVersion, val interface{}) *RWItem {
	return &RWItem{
		Ver: ver,
		Val: val,
	}
}

func (w *RWItem) TxIndex() int {
	return w.Ver.TxIndex
}

func (w *RWItem) TxIncarnation() int {
	return w.Ver.TxIncarnation
}

type PendingWrites struct {
	list []*RWItem
}

func NewPendingWrites() *PendingWrites {
	return &PendingWrites{
		list: make([]*RWItem, 0),
	}
}

func (w *PendingWrites) Append(pw *RWItem) {
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

func (w *PendingWrites) FindLastWrite(txIndex int) *RWItem {
	var i, _ = w.SearchTxIndex(txIndex)
	for j := i - 1; j >= 0; j-- {
		if w.list[j].TxIndex() < txIndex {
			return w.list[j]
		}
	}

	return nil
}

func (w *PendingWrites) FindPrevWrites(txIndex int) []*RWItem {
	var i, _ = w.SearchTxIndex(txIndex)
	for j := i - 1; j >= 0; j-- {
		if w.list[j].TxIndex() < txIndex {
			return w.list[:j+1]
		}
	}

	return nil
}

func (w *PendingWrites) Copy() *PendingWrites {
	np := &PendingWrites{}
	for i, item := range w.list {
		np.list[i] = item
	}
	return np
}

type MVStates struct {
	rwSets              map[int]*RWSet
	pendingAccWriteSet  map[common.Address]map[AccountState]*PendingWrites
	pendingSlotWriteSet map[common.Address]map[common.Hash]*PendingWrites
	nextFinaliseIndex   int

	// dependency map cache for generating TxDAG
	// depMapCache[i].exist(j) means j->i, and i > j
	txDepCache []TxDepMaker

	// async dep analysis
	asyncGenChan  chan int
	asyncStopChan chan struct{}
	asyncRunning  bool

	// execution stat infos
	stats map[int]*ExeStat
	lock  sync.RWMutex
}

func NewMVStates(txCount int) *MVStates {
	return &MVStates{
		rwSets:              make(map[int]*RWSet, txCount),
		pendingAccWriteSet:  make(map[common.Address]map[AccountState]*PendingWrites, txCount*8),
		pendingSlotWriteSet: make(map[common.Address]map[common.Hash]*PendingWrites, txCount*8),
		txDepCache:          make([]TxDepMaker, 0, txCount),
		stats:               make(map[int]*ExeStat, txCount),
	}
}

func (s *MVStates) EnableAsyncGen() *MVStates {
	s.lock.Lock()
	defer s.lock.Unlock()
	chanSize := asyncDepGenChanSize
	if len(s.rwSets) > 0 && len(s.rwSets) < asyncDepGenChanSize {
		chanSize = len(s.rwSets)
	}
	s.asyncGenChan = make(chan int, chanSize)
	s.asyncStopChan = make(chan struct{})
	s.asyncRunning = true
	go s.asyncGenLoop()
	return s
}

func (s *MVStates) Copy() *MVStates {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.asyncGenChan) > 0 {
		log.Error("It's dangerous to copy a async MVStates")
	}
	ns := NewMVStates(len(s.rwSets))
	ns.nextFinaliseIndex = s.nextFinaliseIndex
	ns.txDepCache = append(ns.txDepCache, s.txDepCache...)
	for k, v := range s.rwSets {
		ns.rwSets[k] = v
	}
	for k, v := range s.stats {
		ns.stats[k] = v
	}
	for addr, sub := range s.pendingAccWriteSet {
		for state, writes := range sub {
			if _, ok := ns.pendingAccWriteSet[addr]; !ok {
				ns.pendingAccWriteSet[addr] = make(map[AccountState]*PendingWrites)
			}
			ns.pendingAccWriteSet[addr][state] = writes.Copy()
		}
	}
	for addr, sub := range s.pendingSlotWriteSet {
		for slot, writes := range sub {
			if _, ok := ns.pendingSlotWriteSet[addr]; !ok {
				ns.pendingSlotWriteSet[addr] = make(map[common.Hash]*PendingWrites)
			}
			ns.pendingSlotWriteSet[addr][slot] = writes.Copy()
		}
	}
	return ns
}

func (s *MVStates) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.stopAsyncGen()
	return nil
}

func (s *MVStates) stopAsyncGen() {
	if !s.asyncRunning {
		return
	}
	s.asyncRunning = false
	if s.asyncStopChan != nil {
		close(s.asyncStopChan)
	}
}

func (s *MVStates) asyncGenLoop() {
	for {
		select {
		case tx := <-s.asyncGenChan:
			if err := s.Finalise(tx); err != nil {
				log.Error("async MVStates Finalise err", "err", err)
			}
		case <-s.asyncStopChan:
			return
		}
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

// ReadAccState read state from MVStates
func (s *MVStates) ReadAccState(txIndex int, addr common.Address, state AccountState) *RWItem {
	s.lock.RLock()
	defer s.lock.RUnlock()

	sub, ok := s.pendingAccWriteSet[addr]
	if !ok {
		return nil
	}
	wset, ok := sub[state]
	if !ok {
		return nil
	}
	return wset.FindLastWrite(txIndex)
}

// ReadSlotState read state from MVStates
func (s *MVStates) ReadSlotState(txIndex int, addr common.Address, slot common.Hash) *RWItem {
	s.lock.RLock()
	defer s.lock.RUnlock()

	sub, ok := s.pendingSlotWriteSet[addr]
	if !ok {
		return nil
	}
	wset, ok := sub[slot]
	if !ok {
		return nil
	}
	return wset.FindLastWrite(txIndex)
}

// FulfillRWSet it can execute as async, and rwSet & stat must guarantee read-only
// try to generate TxDAG, when fulfill RWSet
func (s *MVStates) FulfillRWSet(rwSet *RWSet, stat *ExeStat) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	index := rwSet.ver.TxIndex
	if index < s.nextFinaliseIndex {
		return errors.New("fulfill a finalized RWSet")
	}
	if stat != nil {
		if stat.txIndex != index {
			return errors.New("wrong execution stat")
		}
		s.stats[index] = stat
	}
	s.rwSets[index] = rwSet
	return nil
}

// AsyncFinalise it will put target write set into pending writes.
func (s *MVStates) AsyncFinalise(index int) {
	// async resolve dependency, but non-block action
	if s.asyncRunning && s.asyncGenChan != nil {
		select {
		case s.asyncGenChan <- index:
		default:
		}
	}
}

// Finalise it will put target write set into pending writes.
func (s *MVStates) Finalise(index int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// just finalise all previous txs
	for i := s.nextFinaliseIndex; i <= index; i++ {
		if err := s.innerFinalise(i); err != nil {
			return err
		}
		s.resolveDepsMapCacheByWrites(i, s.rwSets[i])
		log.Debug("Finalise the reads/writes", "index", i,
			"readCnt", len(s.rwSets[i].accReadSet)+len(s.rwSets[i].slotReadSet),
			"writeCnt", len(s.rwSets[i].accWriteSet)+len(s.rwSets[i].slotWriteSet))
	}

	return nil
}

func (s *MVStates) innerFinalise(index int) error {
	rwSet := s.rwSets[index]
	if rwSet == nil {
		return fmt.Errorf("finalise a non-exist RWSet, index: %d", index)
	}

	if index != s.nextFinaliseIndex {
		return fmt.Errorf("finalise in wrong order, next: %d, input: %d", s.nextFinaliseIndex, index)
	}

	// append to pending write set
	for addr, sub := range rwSet.accWriteSet {
		if _, exist := s.pendingAccWriteSet[addr]; !exist {
			s.pendingAccWriteSet[addr] = make(map[AccountState]*PendingWrites)
		}
		for state, item := range sub {
			if _, exist := s.pendingAccWriteSet[addr][state]; !exist {
				s.pendingAccWriteSet[addr][state] = NewPendingWrites()
			}
			s.pendingAccWriteSet[addr][state].Append(&item)
		}
	}
	for k, sub := range rwSet.slotWriteSet {
		if _, exist := s.pendingSlotWriteSet[k]; !exist {
			s.pendingSlotWriteSet[k] = make(map[common.Hash]*PendingWrites)
		}
		for slot, item := range sub {
			if _, exist := s.pendingSlotWriteSet[k][slot]; !exist {
				s.pendingSlotWriteSet[k][slot] = NewPendingWrites()
			}
			s.pendingSlotWriteSet[k][slot].Append(&item)
		}
	}
	s.nextFinaliseIndex++
	return nil
}

func (s *MVStates) queryAccWrites(addr common.Address, state AccountState) *PendingWrites {
	if _, exist := s.pendingAccWriteSet[addr]; !exist {
		return nil
	}
	return s.pendingAccWriteSet[addr][state]
}

func (s *MVStates) querySlotWrites(addr common.Address, slot common.Hash) *PendingWrites {
	if _, exist := s.pendingSlotWriteSet[addr]; !exist {
		return nil
	}
	return s.pendingSlotWriteSet[addr][slot]
}

// resolveDepsMapCacheByWrites must be executed in order
func (s *MVStates) resolveDepsMapCacheByWrites(index int, rwSet *RWSet) {
	// analysis dep, if the previous transaction is not executed/validated, re-analysis is required
	depMap := NewTxDepMap(0)
	if rwSet.excludedTx {
		s.txDepCache = append(s.txDepCache, depMap)
		return
	}
	// check tx dependency, only check key, skip version
	for addr, sub := range rwSet.accReadSet {
		for state := range sub {
			// check self destruct
			if state == AccountSelf {
				state = AccountSuicide
			}
			writes := s.queryAccWrites(addr, state)
			if writes == nil {
				continue
			}
			items := writes.FindPrevWrites(index)
			for _, item := range items {
				depMap.add(uint64(item.TxIndex()))
			}
		}
	}
	for addr, sub := range rwSet.slotReadSet {
		for slot := range sub {
			writes := s.querySlotWrites(addr, slot)
			if writes == nil {
				continue
			}
			items := writes.FindPrevWrites(index)
			for _, item := range items {
				depMap.add(uint64(item.TxIndex()))
			}
		}
	}
	// clear redundancy deps compared with prev
	preDeps := depMap.deps()
	for _, prev := range preDeps {
		for _, tx := range s.txDepCache[prev].deps() {
			depMap.remove(tx)
		}
	}
	s.txDepCache = append(s.txDepCache, depMap)
}

// resolveDepsCache must be executed in order
func (s *MVStates) resolveDepsCache(index int, rwSet *RWSet) {
	// analysis dep, if the previous transaction is not executed/validated, re-analysis is required
	depMap := NewTxDepMap(0)
	if rwSet.excludedTx {
		s.txDepCache = append(s.txDepCache, depMap)
		return
	}
	for prev := 0; prev < index; prev++ {
		// if there are some parallel execution or system txs, it will fulfill in advance
		// it's ok, and try re-generate later
		prevSet, ok := s.rwSets[prev]
		if !ok {
			continue
		}
		// if prev tx is tagged ExcludedTxFlag, just skip the check
		if prevSet.excludedTx {
			continue
		}
		// check if there has written op before i
		if checkAccDependency(prevSet.accWriteSet, rwSet.accReadSet) {
			depMap.add(uint64(prev))
			// clear redundancy deps compared with prev
			for _, dep := range depMap.deps() {
				if s.txDepCache[prev].exist(dep) {
					depMap.remove(dep)
				}
			}
		}
		if checkSlotDependency(prevSet.slotWriteSet, rwSet.slotReadSet) {
			depMap.add(uint64(prev))
			// clear redundancy deps compared with prev
			for _, dep := range depMap.deps() {
				if s.txDepCache[prev].exist(dep) {
					depMap.remove(dep)
				}
			}
		}
	}
	s.txDepCache = append(s.txDepCache, depMap)
}

// ResolveTxDAG generate TxDAG from RWSets
func (s *MVStates) ResolveTxDAG(txCnt int, gasFeeReceivers []common.Address) (TxDAG, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.rwSets) != txCnt {
		return nil, fmt.Errorf("wrong rwSet count, expect: %v, actual: %v", txCnt, len(s.rwSets))
	}
	s.stopAsyncGen()
	// collect all rw sets, try to finalise them
	for i := s.nextFinaliseIndex; i < txCnt; i++ {
		if err := s.innerFinalise(i); err != nil {
			return nil, err
		}
	}

	txDAG := NewPlainTxDAG(txCnt)
	for i := 0; i < txCnt; i++ {
		// check if there are RW with gas fee receiver for gas delay calculation
		for _, addr := range gasFeeReceivers {
			if _, ok := s.rwSets[i].accReadSet[addr]; !ok {
				continue
			}
			if _, ok := s.rwSets[i].accReadSet[addr][AccountSelf]; ok {
				return NewEmptyTxDAG(), nil
			}
		}
		txDAG.TxDeps[i].TxIndexes = []uint64{}
		if len(s.txDepCache) <= i {
			s.resolveDepsMapCacheByWrites(i, s.rwSets[i])
		}
		if s.rwSets[i].excludedTx {
			txDAG.TxDeps[i].SetFlag(ExcludedTxFlag)
			continue
		}
		deps := s.txDepCache[i].deps()
		if len(deps) <= (txCnt-1)/2 {
			txDAG.TxDeps[i].TxIndexes = deps
			continue
		}
		// if tx deps larger than half of txs, then convert with NonDependentRelFlag
		txDAG.TxDeps[i].SetFlag(NonDependentRelFlag)
		for j := uint64(0); j < uint64(txCnt); j++ {
			if !slices.Contains(deps, j) && j != uint64(i) {
				txDAG.TxDeps[i].TxIndexes = append(txDAG.TxDeps[i].TxIndexes, j)
			}
		}
	}

	return txDAG, nil
}

func checkAccDependency(writeSet map[common.Address]map[AccountState]RWItem, readSet map[common.Address]map[AccountState]RWItem) bool {
	// check tx dependency, only check key, skip version
	for addr, sub := range writeSet {
		if _, ok := readSet[addr]; !ok {
			continue
		}
		for state := range sub {
			// check suicide, add read address flag, it only for check suicide quickly, and cannot for other scenarios.
			if state == AccountSuicide {
				if _, ok := readSet[addr][AccountSelf]; ok {
					return true
				}
				continue
			}
			if _, ok := readSet[addr][state]; ok {
				return true
			}
		}
	}

	return false
}

func checkSlotDependency(writeSet map[common.Address]map[common.Hash]RWItem, readSet map[common.Address]map[common.Hash]RWItem) bool {
	// check tx dependency, only check key, skip version
	for addr, sub := range writeSet {
		if _, ok := readSet[addr]; !ok {
			continue
		}
		for slot := range sub {
			if _, ok := readSet[addr][slot]; ok {
				return true
			}
		}
	}

	return false
}

type TxDepMaker interface {
	add(index uint64)
	exist(index uint64) bool
	deps() []uint64
	remove(index uint64)
	len() int
}

type TxDepMap struct {
	tm    map[uint64]struct{}
	cache []uint64
}

func NewTxDepMap(cap int) *TxDepMap {
	return &TxDepMap{
		tm: make(map[uint64]struct{}, cap),
	}
}

func (m *TxDepMap) add(index uint64) {
	m.cache = nil
	m.tm[index] = struct{}{}
}

func (m *TxDepMap) exist(index uint64) bool {
	_, ok := m.tm[index]
	return ok
}

func (m *TxDepMap) deps() []uint64 {
	if m.cache != nil {
		return m.cache
	}
	res := make([]uint64, 0, len(m.tm))
	for index := range m.tm {
		res = append(res, index)
	}
	slices.Sort(res)
	m.cache = res
	return m.cache
}

func (m *TxDepMap) remove(index uint64) {
	m.cache = nil
	delete(m.tm, index)
}

func (m *TxDepMap) len() int {
	return len(m.tm)
}
