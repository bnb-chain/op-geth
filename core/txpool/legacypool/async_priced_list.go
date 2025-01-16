package legacypool

import (
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/types"
)

var _ pricedListInterface = &asyncPricedList{}

type addEvent struct {
	tx    *types.Transaction
	local bool
}

type asyncPricedList struct {
	priced         *pricedList
	floatingLowest atomic.Value
	urgentLowest   atomic.Value
	baseFee        atomic.Value
	mu             sync.Mutex

	// events
	quit       chan struct{}
	reheap     chan struct{}
	add        chan *addEvent
	remove     chan int
	setBaseFee chan *big.Int
}

func newAsyncPricedList(all *lookup) *asyncPricedList {
	a := &asyncPricedList{
		priced:     newPricedList(all),
		quit:       make(chan struct{}),
		reheap:     make(chan struct{}),
		add:        make(chan *addEvent),
		remove:     make(chan int),
		setBaseFee: make(chan *big.Int),
	}
	go a.run()
	return a
}

// run is a loop that handles async operations:
//   - reheap: reheap the whole priced list, to get the lowest gas price
//   - put: add a transaction to the priced list
//   - remove: remove transactions from the priced list
//   - discard: remove transactions to make room for new ones
func (a *asyncPricedList) run() {
	var reheap bool
	var newOnes []*types.Transaction
	var toRemove int = 0
	// current loop state
	var currentDone chan struct{} = nil
	var baseFee *big.Int = nil
	for {
		if currentDone == nil {
			currentDone = make(chan struct{})
			go a.handle(reheap, newOnes, toRemove, baseFee, currentDone)
			reheap, newOnes, toRemove, baseFee = false, nil, 0, nil
		}
		select {
		case <-a.reheap:
			reheap = true

		case add := <-a.add:
			newOnes = append(newOnes, add.tx)

		case remove := <-a.remove:
			toRemove += remove

		case baseFee = <-a.setBaseFee:
			// always reheap after setting base fee
			reheap = true

		case <-currentDone:
			currentDone = nil

		case <-a.quit:
			// Wait for current run to finish.
			if currentDone != nil {
				<-currentDone
			}
			return
		}
	}
}

func (a *asyncPricedList) handle(reheap bool, newOnes []*types.Transaction, toRemove int, baseFee *big.Int, finished chan struct{}) {
	defer close(finished)
	a.mu.Lock()
	defer a.mu.Unlock()
	// add new transactions to the priced list
	for _, tx := range newOnes {
		a.priced.Put(tx, false)
	}
	// remove staled transactions from the priced list
	a.priced.Removed(toRemove)
	// reheap if needed
	if reheap {
		a.priced.Reheap()
		// set the lowest priced transaction when reheap is done
		var emptyTx *types.Transaction = nil
		if len(a.priced.floating.list) > 0 {
			a.floatingLowest.Store(a.priced.floating.list[0])
		} else {
			a.floatingLowest.Store(emptyTx)
		}
		if len(a.priced.urgent.list) > 0 {
			a.urgentLowest.Store(a.priced.urgent.list[0])
		} else {
			a.urgentLowest.Store(emptyTx)
		}
	}
	if baseFee != nil {
		a.baseFee.Store(baseFee)
		a.priced.SetBaseFee(baseFee)
	}
}

func (a *asyncPricedList) Staled() int {
	// the Staled() of pricedList is thread-safe, so we don't need to lock here
	return a.priced.Staled()
}

func (a *asyncPricedList) Put(tx *types.Transaction, local bool) {
	select {
	case a.add <- &addEvent{tx, local}:
	case <-a.quit:
	}
}

func (a *asyncPricedList) Removed(count int) {
	select {
	case a.remove <- count:
	case <-a.quit:
	}
}

func (a *asyncPricedList) Underpriced(tx *types.Transaction) bool {
	var urgentLowest, floatingLowest *types.Transaction = nil, nil
	ul, fl := a.urgentLowest.Load(), a.floatingLowest.Load()
	if ul != nil {
		// be careful that ul might be nil
		urgentLowest = ul.(*types.Transaction)
	}
	if fl != nil {
		// be careful that fl might be nil
		floatingLowest = fl.(*types.Transaction)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return (urgentLowest == nil || a.priced.urgent.cmp(urgentLowest, tx) >= 0) &&
		(floatingLowest == nil || a.priced.floating.cmp(floatingLowest, tx) >= 0) &&
		(floatingLowest != nil || urgentLowest != nil)
}

// Disacard cleans staled transactions to make room for new ones
func (a *asyncPricedList) Discard(slots int, force bool) (types.Transactions, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.priced.Discard(slots, force)
}

func (a *asyncPricedList) NeedReheap(currHead *types.Header) bool {
	return false
}

func (a *asyncPricedList) Reheap() {
	select {
	case a.reheap <- struct{}{}:
	case <-a.quit:
	}
}

func (a *asyncPricedList) SetBaseFee(baseFee *big.Int) {
	select {
	case a.setBaseFee <- baseFee:
	case <-a.quit:
	}
}

func (a *asyncPricedList) SetHead(currHead *types.Header) {
	//do nothing
}

func (a *asyncPricedList) GetBaseFee() *big.Int {
	baseFee := a.baseFee.Load()
	if baseFee == nil {
		return big.NewInt(0)
	}
	return baseFee.(*big.Int)
}

func (a *asyncPricedList) Close() {
	close(a.quit)
}

func (a *asyncPricedList) TxCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.priced.TxCount()
}
