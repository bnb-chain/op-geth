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

type discardEvent struct {
	slots int
	force bool
	done  chan *discardResult
}
type discardResult struct {
	discardTxs types.Transactions
	succ       bool
}

type asyncPricedList struct {
	priced         *pricedList
	floatingLowest atomic.Value
	urgentLowest   atomic.Value
	mu             sync.Mutex

	// events
	quit       chan struct{}
	reheap     chan struct{}
	add        chan *addEvent
	remove     chan int
	discard    chan *discardEvent
	setBaseFee chan *big.Int
}

func newAsyncPricedList(all *lookup) *asyncPricedList {
	a := &asyncPricedList{
		priced:     newPricedList(all),
		quit:       make(chan struct{}),
		reheap:     make(chan struct{}),
		add:        make(chan *addEvent),
		remove:     make(chan int),
		discard:    make(chan *discardEvent),
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
			go func(reheap bool, newOnes []*types.Transaction, toRemove int, baseFee *big.Int) {
				a.handle(reheap, newOnes, toRemove, baseFee, currentDone)
				<-currentDone
				close(currentDone)
				currentDone = nil
			}(reheap, newOnes, toRemove, baseFee)

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

		case <-a.quit:
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
		if len(a.priced.floating.list) > 0 {
			a.floatingLowest.Store(a.priced.floating.list[0])
		} else {
			a.floatingLowest.Store(nil)
		}
		if len(a.priced.urgent.list) > 0 {
			a.urgentLowest.Store(a.priced.urgent.list[0])
		} else {
			a.urgentLowest.Store(nil)
		}
	}
	if baseFee != nil {
		a.priced.SetBaseFee(baseFee)
	}
}

func (a *asyncPricedList) Put(tx *types.Transaction, local bool) {
	a.add <- &addEvent{tx, local}
}

func (a *asyncPricedList) Removed(count int) {
	a.remove <- count
}

func (a *asyncPricedList) Underpriced(tx *types.Transaction) bool {
	urgentLowest, floatingLowest := a.urgentLowest.Load(), a.floatingLowest.Load()
	return (urgentLowest == nil || a.priced.urgent.cmp(urgentLowest.(*types.Transaction), tx) >= 0) &&
		(floatingLowest == nil || a.priced.floating.cmp(floatingLowest.(*types.Transaction), tx) >= 0) &&
		(floatingLowest != nil && urgentLowest != nil)
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
	a.reheap <- struct{}{}
}

func (a *asyncPricedList) SetBaseFee(baseFee *big.Int) {
	a.setBaseFee <- baseFee
	a.reheap <- struct{}{}
}

func (a *asyncPricedList) SetHead(currHead *types.Header) {
	//do nothing
}

func (a *asyncPricedList) GetBaseFee() *big.Int {
	return a.priced.floating.baseFee
}

func (a *asyncPricedList) Stop() {
	close(a.quit)
}
