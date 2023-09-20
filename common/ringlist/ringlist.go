package ringlist

// RingList for for pending tx to be reannounced, which ensures that every address in pending pool has equal chances
// to be reannounced
type RingList[T comparable] struct {
	entry    *RingNode[T]
	preEntry *RingNode[T] //the pointer of pre-node of entry
	toRemove map[T]bool   // values to be removed by Clean()
	exists   map[T]bool   // to avoid duplicate element enqueue the list
}

type RingNode[T comparable] struct {
	val  T
	next *RingNode[T]
}

func NewRingList[T comparable]() *RingList[T] {
	return &RingList[T]{
		toRemove: make(map[T]bool),
		exists:   make(map[T]bool),
	}
}

// Add a new addr into ring. If already included, it will be ignored
func (rc *RingList[T]) Add(value T) {
	if _, ok := rc.exists[value]; ok {
		return
	}
	newone := &RingNode[T]{
		val: value,
	}
	defer func() { rc.exists[value] = true }()
	if rc.Len() == 0 {
		rc.entry, rc.preEntry = newone, newone
		rc.entry.next = newone
		return
	}
	if rc.Len() == 1 {
		rc.preEntry.next, rc.entry, newone.next = newone, newone, rc.entry.next
		return

	} else {
		newone.next = rc.entry.next
		rc.preEntry, rc.entry = rc.entry, newone
		rc.preEntry.next = rc.entry
		return
	}
}

// Mark an addr to be removed later
func (rc *RingList[T]) MarkRemoved(values ...T) {
	for _, val := range values {
		rc.toRemove[val] = true
	}
}

// Purge all addrs marked to be removed
func (rc *RingList[T]) Purge() {
	defer func() { rc.toRemove = make(map[T]bool) }()
	total := rc.Len()
	preEntry, entry := rc.preEntry, rc.entry
	for i := 0; i < total; i++ {
		if _, ok := rc.toRemove[entry.val]; ok {
			rc.trim(entry, preEntry)
			delete(rc.toRemove, entry.val)
			entry = entry.next
		} else {
			preEntry, entry = entry, entry.next
		}
	}

}

// return the value holded by entry and then move the entry to next position.
func (rc *RingList[T]) Next(defaultVal T) T {
	if rc.entry == nil {
		return defaultVal
	}
	curr := rc.entry.val
	rc.preEntry, rc.entry = rc.entry, rc.entry.next
	return curr
}

func (rc *RingList[T]) Len() int {
	return len(rc.exists)
}

func (rc *RingList[T]) trim(entry, preEntry *RingNode[T]) {
	if rc.Len() == 0 {
		return
	}
	curr := entry.val
	defer func() { delete(rc.exists, curr) }()

	//only one element left, clear all status
	if rc.Len() == 1 {
		rc.entry, rc.preEntry = nil, nil
		return
	}

	// remove current element from the ring
	preEntry.next = entry.next

	//update the rc.entry if it's hit
	if entry == rc.entry {
		rc.entry = preEntry.next
		return
	}

	//update the rc.preEntry if it's hit
	if entry == rc.preEntry {
		rc.preEntry = preEntry
		return
	}
}
