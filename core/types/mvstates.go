package types

import (
	"fmt"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
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
	initRWEventCacheSize = 4
)

func init() {
	for i := 0; i < initRWEventCacheSize; i++ {
		cache := make([]RWEventItem, 10000)
		rwEventCachePool.Put(&cache)
	}
}

// RWSet record all read & write set in txs
// Attention: this is not a concurrent safety structure
type RWSet struct {
	index        int
	accReadSet   map[common.Address]map[AccountState]struct{}
	accWriteSet  map[common.Address]map[AccountState]struct{}
	slotReadSet  map[common.Address]map[common.Hash]struct{}
	slotWriteSet map[common.Address]map[common.Hash]struct{}

	// some flags
	excludedTx        bool
	cannotGasFeeDelay bool
}

func NewRWSet(index int) *RWSet {
	return &RWSet{
		index:        index,
		accReadSet:   make(map[common.Address]map[AccountState]struct{}),
		accWriteSet:  make(map[common.Address]map[AccountState]struct{}),
		slotReadSet:  make(map[common.Address]map[common.Hash]struct{}),
		slotWriteSet: make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (s *RWSet) RecordAccountRead(addr common.Address, state AccountState) {
	// only record the first read version
	sub, ok := s.accReadSet[addr]
	if !ok {
		s.accReadSet[addr] = make(map[AccountState]struct{})
		s.accReadSet[addr][AccountSelf] = struct{}{}
		s.accReadSet[addr][state] = struct{}{}
		return
	}
	if _, ok = sub[state]; ok {
		return
	}
	s.accReadSet[addr][state] = struct{}{}
}

func (s *RWSet) RecordStorageRead(addr common.Address, slot common.Hash) {
	// only record the first read version
	sub, ok := s.slotReadSet[addr]
	if !ok {
		s.slotReadSet[addr] = make(map[common.Hash]struct{})
		s.slotReadSet[addr][slot] = struct{}{}
		return
	}
	if _, ok = sub[slot]; ok {
		return
	}
	s.slotReadSet[addr][slot] = struct{}{}
}

func (s *RWSet) RecordAccountWrite(addr common.Address, state AccountState) {
	_, ok := s.accWriteSet[addr]
	if !ok {
		s.accWriteSet[addr] = make(map[AccountState]struct{})
	}
	s.accWriteSet[addr][state] = struct{}{}
}

func (s *RWSet) RecordStorageWrite(addr common.Address, slot common.Hash) {
	_, ok := s.slotWriteSet[addr]
	if !ok {
		s.slotWriteSet[addr] = make(map[common.Hash]struct{})
	}
	s.slotWriteSet[addr][slot] = struct{}{}
}

func (s *RWSet) ReadSet() (map[common.Address]map[AccountState]struct{}, map[common.Address]map[common.Hash]struct{}) {
	return s.accReadSet, s.slotReadSet
}

func (s *RWSet) WriteSet() (map[common.Address]map[AccountState]struct{}, map[common.Address]map[common.Hash]struct{}) {
	return s.accWriteSet, s.slotWriteSet
}

func (s *RWSet) WithExcludedTxFlag() *RWSet {
	s.excludedTx = true
	return s
}

func (s *RWSet) String() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("{tx: %v", s.index))
	builder.WriteString(", accReadSet: [")
	i := 0
	for addr, sub := range s.accReadSet {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("{addr: \"%v\", states: [", addr))
		j := 0
		for key := range sub {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("%v", key))
			j++
		}
		i++
		builder.WriteString("]}")
	}
	builder.WriteString("], slotReadSet: [")
	i = 0
	for addr, sub := range s.slotReadSet {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("{addr: \"%v\", slots: [", addr))
		j := 0
		for key := range sub {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("\"%v\"", key.String()))
			j++
		}
		i++
		builder.WriteString("]}")
	}
	builder.WriteString("], accWriteSet: [")
	i = 0
	for addr, sub := range s.accWriteSet {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("{addr: \"%v\", states: [", addr))
		j := 0
		for key := range sub {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("%v", key))
			j++
		}
		i++
		builder.WriteString("]}")
	}
	builder.WriteString("], slotWriteSet: [")
	i = 0
	for addr, sub := range s.slotWriteSet {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("{addr: \"%v\", slots: [", addr))
		j := 0
		for key := range sub {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("\"%v\"", key.String()))
			j++
		}
		i++
		builder.WriteString("]}")
	}
	builder.WriteString("]}")
	return builder.String()
}

const (
	NewTxRWEvent byte = iota
	ReadAccRWEvent
	WriteAccRWEvent
	ReadSlotRWEvent
	WriteSlotRWEvent
	CannotGasFeeDelayRWEvent
)

type RWEventItem struct {
	Event byte
	Index int
	Addr  common.Address
	State AccountState
	Slot  common.Hash
}

type StateWrites struct {
	list []int
}

func NewStateWrites() *StateWrites {
	return &StateWrites{
		list: make([]int, 0),
	}
}

func (w *StateWrites) Append(pw int) {
	if i, found := w.SearchTxIndex(pw); found {
		w.list[i] = pw
		return
	}

	w.list = append(w.list, pw)
	for i := len(w.list) - 1; i > 0; i-- {
		if w.list[i] > w.list[i-1] {
			break
		}
		w.list[i-1], w.list[i] = w.list[i], w.list[i-1]
	}
}

func (w *StateWrites) SearchTxIndex(txIndex int) (int, bool) {
	n := len(w.list)
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		// i ≤ h < j
		if w.list[h] < txIndex {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < n && w.list[i] == txIndex
}

func (w *StateWrites) FindLastWrite(txIndex int) int {
	var i, _ = w.SearchTxIndex(txIndex)
	for j := i - 1; j >= 0; j-- {
		if w.list[j] < txIndex {
			return w.list[j]
		}
	}

	return -1
}

func (w *StateWrites) Copy() *StateWrites {
	np := &StateWrites{}
	for i, item := range w.list {
		np.list[i] = item
	}
	return np
}

var (
	rwEventCachePool = sync.Pool{New: func() any {
		buf := make([]RWEventItem, 0)
		return &buf
	}}
)

type MVStates struct {
	rwSets              map[int]*RWSet
	pendingAccWriteSet  map[common.Address]map[AccountState]*StateWrites
	pendingSlotWriteSet map[common.Address]map[common.Hash]*StateWrites
	nextFinaliseIndex   int
	gasFeeReceivers     []common.Address
	// dependency map cache for generating TxDAG
	// depMapCache[i].exist(j) means j->i, and i > j
	txDepCache map[int]TxDep
	lock       sync.RWMutex

	// async rw event recorder
	// these fields are only used in one routine
	asyncRWSet        *RWSet
	rwEventCh         chan []RWEventItem
	rwEventCache      []RWEventItem
	rwEventCacheIndex int
	recordingRead     bool
	recordingWrite    bool
	asyncRunning      bool
	asyncWG           sync.WaitGroup
}

func NewMVStates(txCount int, gasFeeReceivers []common.Address) *MVStates {
	m := &MVStates{
		rwSets:              make(map[int]*RWSet, txCount),
		pendingAccWriteSet:  make(map[common.Address]map[AccountState]*StateWrites, txCount),
		pendingSlotWriteSet: make(map[common.Address]map[common.Hash]*StateWrites, txCount),
		txDepCache:          make(map[int]TxDep, txCount),
		rwEventCh:           make(chan []RWEventItem, 100),
		gasFeeReceivers:     gasFeeReceivers,
	}
	m.rwEventCache = *rwEventCachePool.Get().(*[]RWEventItem)
	m.rwEventCache = m.rwEventCache[:cap(m.rwEventCache)]
	m.rwEventCacheIndex = 0
	return m
}

func (s *MVStates) EnableAsyncGen() *MVStates {
	s.asyncWG.Add(1)
	s.asyncRunning = true
	go s.asyncRWEventLoop()
	return s
}

func (s *MVStates) Stop() {
	s.stopAsyncRecorder()
}

func (s *MVStates) Copy() *MVStates {
	s.lock.Lock()
	defer s.lock.Unlock()
	ns := NewMVStates(len(s.rwSets), s.gasFeeReceivers)
	ns.nextFinaliseIndex = s.nextFinaliseIndex
	for k, v := range s.txDepCache {
		ns.txDepCache[k] = v
	}
	for k, v := range s.rwSets {
		ns.rwSets[k] = v
	}
	for addr, sub := range s.pendingAccWriteSet {
		for state, writes := range sub {
			if _, ok := ns.pendingAccWriteSet[addr]; !ok {
				ns.pendingAccWriteSet[addr] = make(map[AccountState]*StateWrites)
			}
			ns.pendingAccWriteSet[addr][state] = writes.Copy()
		}
	}
	for addr, sub := range s.pendingSlotWriteSet {
		for slot, writes := range sub {
			if _, ok := ns.pendingSlotWriteSet[addr]; !ok {
				ns.pendingSlotWriteSet[addr] = make(map[common.Hash]*StateWrites)
			}
			ns.pendingSlotWriteSet[addr][slot] = writes.Copy()
		}
	}
	return ns
}

func (s *MVStates) asyncRWEventLoop() {
	defer s.asyncWG.Done()
	for {
		select {
		case items, ok := <-s.rwEventCh:
			if !ok {
				return
			}
			for _, item := range items {
				s.handleRWEvent(item)
			}
			rwEventCachePool.Put(&items)
		}
	}
}

func (s *MVStates) handleRWEvent(item RWEventItem) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// init next RWSet, and finalise previous RWSet
	if item.Event == NewTxRWEvent {
		s.finalisePreviousRWSet()
		s.asyncRWSet = NewRWSet(item.Index)
		return
	}
	if s.asyncRWSet == nil {
		return
	}
	switch item.Event {
	// recorde current read/write event
	case ReadAccRWEvent:
		s.asyncRWSet.RecordAccountRead(item.Addr, item.State)
	case ReadSlotRWEvent:
		s.asyncRWSet.RecordStorageRead(item.Addr, item.Slot)
	case WriteAccRWEvent:
		s.finaliseAccWrite(s.asyncRWSet.index, item.Addr, item.State)
	case WriteSlotRWEvent:
		s.finaliseSlotWrite(s.asyncRWSet.index, item.Addr, item.Slot)
	// recorde current as cannot gas fee delay
	case CannotGasFeeDelayRWEvent:
		s.asyncRWSet.cannotGasFeeDelay = true
	}
}

func (s *MVStates) finalisePreviousRWSet() {
	if s.asyncRWSet == nil {
		return
	}
	index := s.asyncRWSet.index
	s.rwSets[index] = s.asyncRWSet

	// check if there are RW with gas fee receiver for gas delay calculation
	for _, addr := range s.gasFeeReceivers {
		if _, exist := s.asyncRWSet.accReadSet[addr]; !exist {
			continue
		}
		if _, exist := s.asyncRWSet.accReadSet[addr][AccountSelf]; exist {
			s.rwSets[index].cannotGasFeeDelay = true
			break
		}
	}
	if err := s.innerFinalise(index, false); err != nil {
		log.Error("Finalise err when handle NewTxRWEvent", "tx", index, "err", err)
		return
	}
	s.resolveDepsMapCacheByWrites(index, s.asyncRWSet)
}

func (s *MVStates) RecordNewTx(index int) {
	if !s.asyncRunning {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = NewTxRWEvent
		s.rwEventCache[s.rwEventCacheIndex].Index = index
	} else {
		s.rwEventCache = append(s.rwEventCache, RWEventItem{
			Event: NewTxRWEvent,
			Index: index,
		})
	}
	s.rwEventCacheIndex++
	s.recordingRead = true
	s.recordingWrite = true
	if index%10 == 0 {
		s.BatchRecordHandle()
	}
}

func (s *MVStates) RecordReadDone() {
	s.recordingRead = false
}

func (s *MVStates) RecordWriteDone() {
	s.recordingWrite = false
}

func (s *MVStates) RecordAccountRead(addr common.Address, state AccountState) {
	if !s.asyncRunning || !s.recordingRead {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = ReadAccRWEvent
		s.rwEventCache[s.rwEventCacheIndex].Addr = addr
		s.rwEventCache[s.rwEventCacheIndex].State = state
		s.rwEventCacheIndex++
		return
	}
	s.rwEventCache = append(s.rwEventCache, RWEventItem{
		Event: ReadAccRWEvent,
		Addr:  addr,
		State: state,
	})
	s.rwEventCacheIndex++
}

func (s *MVStates) RecordStorageRead(addr common.Address, slot common.Hash) {
	if !s.asyncRunning || !s.recordingRead {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = ReadSlotRWEvent
		s.rwEventCache[s.rwEventCacheIndex].Addr = addr
		s.rwEventCache[s.rwEventCacheIndex].Slot = slot
		s.rwEventCacheIndex++
		return
	}
	s.rwEventCache = append(s.rwEventCache, RWEventItem{
		Event: ReadSlotRWEvent,
		Addr:  addr,
		Slot:  slot,
	})
	s.rwEventCacheIndex++
}

func (s *MVStates) RecordAccountWrite(addr common.Address, state AccountState) {
	if !s.asyncRunning || !s.recordingWrite {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = WriteAccRWEvent
		s.rwEventCache[s.rwEventCacheIndex].Addr = addr
		s.rwEventCache[s.rwEventCacheIndex].State = state
		s.rwEventCacheIndex++
		return
	}
	s.rwEventCache = append(s.rwEventCache, RWEventItem{
		Event: WriteAccRWEvent,
		Addr:  addr,
		State: state,
	})
	s.rwEventCacheIndex++
}

func (s *MVStates) RecordStorageWrite(addr common.Address, slot common.Hash) {
	if !s.asyncRunning || !s.recordingWrite {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = WriteSlotRWEvent
		s.rwEventCache[s.rwEventCacheIndex].Addr = addr
		s.rwEventCache[s.rwEventCacheIndex].Slot = slot
		s.rwEventCacheIndex++
		return
	}
	s.rwEventCache = append(s.rwEventCache, RWEventItem{
		Event: WriteSlotRWEvent,
		Addr:  addr,
		Slot:  slot,
	})
	s.rwEventCacheIndex++
}

func (s *MVStates) RecordCannotDelayGasFee() {
	if !s.asyncRunning || !s.recordingWrite {
		return
	}
	if s.rwEventCacheIndex < len(s.rwEventCache) {
		s.rwEventCache[s.rwEventCacheIndex].Event = CannotGasFeeDelayRWEvent
		s.rwEventCacheIndex++
		return
	}
	s.rwEventCache = append(s.rwEventCache, RWEventItem{
		Event: CannotGasFeeDelayRWEvent,
	})
	s.rwEventCacheIndex++
}

func (s *MVStates) BatchRecordHandle() {
	if !s.asyncRunning || s.rwEventCacheIndex == 0 {
		return
	}
	s.rwEventCh <- s.rwEventCache[:s.rwEventCacheIndex]
	s.rwEventCache = *rwEventCachePool.Get().(*[]RWEventItem)
	s.rwEventCache = s.rwEventCache[:cap(s.rwEventCache)]
	s.rwEventCacheIndex = 0
}

func (s *MVStates) stopAsyncRecorder() {
	if s.asyncRunning {
		s.BatchRecordHandle()
		s.asyncRunning = false
		close(s.rwEventCh)
		s.asyncWG.Wait()
	}
}

// FinaliseWithRWSet it will put target write set into pending writes.
func (s *MVStates) FinaliseWithRWSet(rwSet *RWSet) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	index := rwSet.index
	s.rwSets[index] = rwSet
	// just finalise all previous txs
	start := s.nextFinaliseIndex
	if start > index {
		start = index
	}
	for i := start; i <= index; i++ {
		if err := s.innerFinalise(i, true); err != nil {
			return err
		}
		s.resolveDepsMapCacheByWrites(i, s.rwSets[i])
		//log.Debug("Finalise the reads/writes", "index", i,
		//	"readCnt", len(s.rwSets[i].accReadSet)+len(s.rwSets[i].slotReadSet),
		//	"writeCnt", len(s.rwSets[i].accWriteSet)+len(s.rwSets[i].slotWriteSet))
	}

	return nil
}

func (s *MVStates) innerFinalise(index int, applyWriteSet bool) error {
	rwSet := s.rwSets[index]
	if rwSet == nil {
		return fmt.Errorf("finalise a non-exist RWSet, index: %d", index)
	}

	if index > s.nextFinaliseIndex {
		return fmt.Errorf("finalise in wrong order, next: %d, input: %d", s.nextFinaliseIndex, index)
	}

	// reset nextFinaliseIndex to index+1, it may revert to previous txs
	s.nextFinaliseIndex = index + 1
	if !applyWriteSet {
		return nil
	}

	// append to pending write set
	for addr, sub := range rwSet.accWriteSet {
		if _, exist := s.pendingAccWriteSet[addr]; !exist {
			s.pendingAccWriteSet[addr] = make(map[AccountState]*StateWrites)
		}
		for state := range sub {
			if _, exist := s.pendingAccWriteSet[addr][state]; !exist {
				s.pendingAccWriteSet[addr][state] = NewStateWrites()
			}
			s.pendingAccWriteSet[addr][state].Append(index)
		}
	}
	for addr, sub := range rwSet.slotWriteSet {
		if _, exist := s.pendingSlotWriteSet[addr]; !exist {
			s.pendingSlotWriteSet[addr] = make(map[common.Hash]*StateWrites)
		}
		for slot := range sub {
			if _, exist := s.pendingSlotWriteSet[addr][slot]; !exist {
				s.pendingSlotWriteSet[addr][slot] = NewStateWrites()
			}
			s.pendingSlotWriteSet[addr][slot].Append(index)
		}
	}
	return nil
}

func (s *MVStates) finaliseSlotWrite(index int, addr common.Address, slot common.Hash) {
	// append to pending write set
	if _, exist := s.pendingSlotWriteSet[addr]; !exist {
		s.pendingSlotWriteSet[addr] = make(map[common.Hash]*StateWrites)
	}
	if _, exist := s.pendingSlotWriteSet[addr][slot]; !exist {
		s.pendingSlotWriteSet[addr][slot] = NewStateWrites()
	}
	s.pendingSlotWriteSet[addr][slot].Append(index)
}

func (s *MVStates) finaliseAccWrite(index int, addr common.Address, state AccountState) {
	// append to pending write set
	if _, exist := s.pendingAccWriteSet[addr]; !exist {
		s.pendingAccWriteSet[addr] = make(map[AccountState]*StateWrites)
	}
	if _, exist := s.pendingAccWriteSet[addr][state]; !exist {
		s.pendingAccWriteSet[addr][state] = NewStateWrites()
	}
	s.pendingAccWriteSet[addr][state].Append(index)
}

func (s *MVStates) queryAccWrites(addr common.Address, state AccountState) *StateWrites {
	if _, exist := s.pendingAccWriteSet[addr]; !exist {
		return nil
	}
	return s.pendingAccWriteSet[addr][state]
}

func (s *MVStates) querySlotWrites(addr common.Address, slot common.Hash) *StateWrites {
	if _, exist := s.pendingSlotWriteSet[addr]; !exist {
		return nil
	}
	return s.pendingSlotWriteSet[addr][slot]
}

// resolveDepsMapCacheByWrites must be executed in order
func (s *MVStates) resolveDepsMapCacheByWrites(index int, rwSet *RWSet) {
	// analysis dep, if the previous transaction is not executed/validated, re-analysis is required
	if rwSet.excludedTx {
		s.txDepCache[index] = NewTxDep([]uint64{}, ExcludedTxFlag)
		return
	}
	depMap := NewTxDepMap(0)
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
			find := writes.FindLastWrite(index)
			if find < 0 {
				continue
			}
			tx := uint64(find)
			if depMap.exist(tx) {
				continue
			}
			depMap.add(tx)
		}
	}
	for addr, sub := range rwSet.slotReadSet {
		for slot := range sub {
			writes := s.querySlotWrites(addr, slot)
			if writes == nil {
				continue
			}
			find := writes.FindLastWrite(index)
			if find < 0 {
				continue
			}
			tx := uint64(find)
			if depMap.exist(tx) {
				continue
			}
			depMap.add(tx)
		}
	}
	//log.Debug("resolveDepsMapCacheByWrites", "tx", index, "deps", depMap.deps())
	// clear redundancy deps compared with prev
	preDeps := depMap.deps()
	for _, prev := range preDeps {
		for _, tx := range s.txDepCache[int(prev)].TxIndexes {
			depMap.remove(tx)
		}
	}
	//log.Debug("resolveDepsMapCacheByWrites after clean", "tx", index, "deps", depMap.deps())
	s.txDepCache[index] = NewTxDep(depMap.deps())
}

// resolveDepsCache must be executed in order
func (s *MVStates) resolveDepsCache(index int, rwSet *RWSet) {
	// analysis dep, if the previous transaction is not executed/validated, re-analysis is required
	if rwSet.excludedTx {
		s.txDepCache[index] = NewTxDep([]uint64{}, ExcludedTxFlag)
		return
	}
	depMap := NewTxDepMap(0)
	for prev := 0; prev < index; prev++ {
		// if there are some parallel execution or system txs, it will fulfill in advance
		// it's ok, and try re-generate later
		prevSet := s.rwSets[prev]
		if prevSet == nil {
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
				if slices.Contains(s.txDepCache[prev].TxIndexes, dep) {
					depMap.remove(dep)
				}
			}
		}
		if checkSlotDependency(prevSet.slotWriteSet, rwSet.slotReadSet) {
			depMap.add(uint64(prev))
			// clear redundancy deps compared with prev
			for _, dep := range depMap.deps() {
				if slices.Contains(s.txDepCache[prev].TxIndexes, dep) {
					depMap.remove(dep)
				}
			}
		}
	}
	s.txDepCache[index] = NewTxDep(depMap.deps())
}

// ResolveTxDAG generate TxDAG from RWSets
func (s *MVStates) ResolveTxDAG(txCnt int) (TxDAG, error) {
	s.stopAsyncRecorder()

	s.lock.Lock()
	defer s.lock.Unlock()
	s.finalisePreviousRWSet()
	if s.nextFinaliseIndex != txCnt {
		return nil, fmt.Errorf("cannot resolve with wrong FinaliseIndex, expect: %v, now: %v", txCnt, s.nextFinaliseIndex)
	}

	txDAG := NewPlainTxDAG(txCnt)
	for i := 0; i < txCnt; i++ {
		if s.rwSets[i].cannotGasFeeDelay {
			return NewEmptyTxDAG(), nil
		}
		deps := s.txDepCache[i].TxIndexes
		if len(deps) <= (txCnt-1)/2 {
			txDAG.TxDeps[i] = s.txDepCache[i]
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

func (s *MVStates) FeeReceivers() []common.Address {
	return s.gasFeeReceivers
}

func checkAccDependency(writeSet map[common.Address]map[AccountState]struct{}, readSet map[common.Address]map[AccountState]struct{}) bool {
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

func checkSlotDependency(writeSet map[common.Address]map[common.Hash]struct{}, readSet map[common.Address]map[common.Hash]struct{}) bool {
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
