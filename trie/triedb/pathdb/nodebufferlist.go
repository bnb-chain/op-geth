package pathdb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

const (
	// mergeBlockInterval defines the interval of block in one node cache.
	mergeBlockInterval = 1800

	// mergeNodeCacheInterval defines the interval to collect nodes to flush disk.
	mergeNodeCacheInterval = 3

	// reserveNodeCacheNumber defines the reserve number of node cache in node list.
	reserveNodeCacheNumber = 3
)

var _ trienodebuffer = &nodebufferlist{}

var nodeCachePool = sync.Pool{
	New: func() interface{} {
		return new(nodecache)
	},
}

// nodebufferlist implements the trienodebuffer interface, It is designed to meet
// the withdraw proof function of opBNB at the storage layer while taking into
// account high performance. It is a nodecache based queue that stores
// mergeBlockInterval compressed block difflayers per nodecache. It also has one
// base nodecache that collects the list's trie nodes to write disk.
type nodebufferlist struct {
	db    ethdb.Database
	clean *fastcache.Cache
	limit uint64
	block uint64

	head *nodecache
	tail *nodecache

	mux       sync.RWMutex
	basemMux  sync.RWMutex
	base      *nodecache
	size      uint64
	count     uint64
	persistID uint64
	layers    uint64

	isFlushing   atomic.Bool
	stopFlushing atomic.Bool
	stopCh       chan struct{}
}

// newNodeBufferList initializes the node buffer list with the provided nodes
func newNodeBufferList(
	db ethdb.Database,
	limit uint64,
	nodes map[common.Hash]map[string]*trienode.Node,
	layers uint64) *nodebufferlist {
	if nodes == nil {
		nodes = make(map[common.Hash]map[string]*trienode.Node)
	}
	var size uint64
	for _, subset := range nodes {
		for path, n := range subset {
			size += uint64(len(n.Blob) + len(path))
		}
	}
	nc := newNodeCache(limit, size, nodes, layers)
	ele := newNodeCache(limit, 0, make(map[common.Hash]map[string]*trienode.Node), 0)
	nf := &nodebufferlist{
		db:        db,
		limit:     limit,
		base:      nc,
		head:      ele,
		tail:      ele,
		count:     1,
		persistID: rawdb.ReadPersistentStateID(db),
		stopCh:    make(chan struct{}),
	}
	go nf.loop()
	return nf
}

// node retrieves the trie node with given node info.
func (nf *nodebufferlist) node(owner common.Hash, path []byte, hash common.Hash) (node *trienode.Node, err error) {
	nf.mux.RLock()
	find := func(nc *nodecache) bool {
		subset, ok := nc.nodes[owner]
		if !ok {
			return true
		}
		n, ok := subset[string(path)]
		if !ok {
			return true
		}
		if n.Hash != hash {
			log.Error("Unexpected trie node in node buffer list", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
			err = newUnexpectedNodeError("dirty", hash, n.Hash, owner, path, n.Blob)
			return false
		}
		node = n
		return false
	}
	nf.traverse(find)
	if err != nil {
		nf.mux.RUnlock()
		return nil, err
	}
	if node != nil {
		nf.mux.RUnlock()
		return node, nil
	}
	nf.mux.RUnlock()

	nf.basemMux.RLock()
	node, err = nf.base.node(owner, path, hash)
	nf.basemMux.RUnlock()
	return node, err
}

// commit merges the dirty nodes into the trienodebuffer. This operation won't take
// the ownership of the nodes map which belongs to the bottom-most diff layer.
// It will just hold the node references from the given map which are safe to
// copy.
func (nf *nodebufferlist) commit(block uint64, nodes map[common.Hash]map[string]*trienode.Node) trienodebuffer {
	nf.mux.Lock()
	defer nf.mux.Unlock()

	oldSize := nf.head.size
	err := nf.head.commit(block, nodes, false)
	if err != nil {
		log.Crit("failed to commit nodes to node buffer list", "error", err)
	}

	nf.block = block
	nf.size = nf.size + nf.head.size - oldSize
	nf.layers++

	nodeBufferLastBlock.Mark(int64(nf.block))
	nodeBufferListSizeMeter.Mark(int64(nf.size))
	nodeBufferLayerMeter.Mark(int64(nf.layers))

	if block != 0 && block%mergeBlockInterval == 0 {
		nc := nodeCachePool.Get().(*nodecache)
		nc.reset()
		nf.pushFront(nc)
	}
	return nf
}

// revert is the reverse operation of commit. It also merges the provided nodes
// into the trienodebuffer, the difference is that the provided node set should
// revert the changes made by the last state transition.
func (nf *nodebufferlist) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) error {
	// hang user read/write and background write,
	nf.mux.Lock()
	nf.basemMux.Lock()
	defer nf.mux.Unlock()
	defer nf.basemMux.Unlock()

	merge := func(buffer *nodecache) bool {
		if err := nf.base.commit(buffer.block, buffer.nodes, false); err != nil {
			log.Crit("failed to commit nodes to base node buffer", "error", err)
		}
		baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
		baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))

		del := nf.popBack()
		nf.base.layers--
		nf.base.layers += del.layers
		nodeCachePool.Put(del)
		return true
	}
	nf.traverseReverse(merge)
	nc := nodeCachePool.Get().(*nodecache)
	nc.reset()
	nf.head = nc
	nf.tail = nc

	return nf.base.revert(nf.db, nodes)
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (nf *nodebufferlist) flush(db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64, force bool) error {
	if nf.clean == nil {
		nf.clean = clean
	}
	if !force {
		return nil
	}

	// hang user read/write and background write
	nf.mux.Lock()
	nf.basemMux.Lock()
	defer nf.mux.Unlock()
	defer nf.basemMux.Unlock()

	nf.stopFlushing.Store(true)
	defer nf.stopFlushing.Store(false)
	for {
		if nf.isFlushing.Swap(true) {
			time.Sleep(time.Duration(DefaultBackgroundFlushInterval) * time.Second)
			log.Info("waiting base node cache flushed into disk for forcing flush node buffer")
			continue
		} else {
			break
		}
	}

	commitFunc := func(buffer *nodecache) bool {
		if err := nf.base.commit(buffer.block, buffer.nodes, false); err != nil {
			log.Crit("failed to commit nodes to base node buffer", "error", err)
		}
		del := nf.popBack()
		nf.base.layers--
		nf.base.layers += del.layers

		baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
		baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))

		nodeCachePool.Put(del)
		return true
	}
	nf.traverseReverse(commitFunc)
	persistID := nf.persistID + nf.base.layers
	err := nf.base.flush(nf.db, nf.clean, persistID, false)
	if err != nil {
		log.Crit("failed to flush base node buffer to disk", "error", err)
	}
	nf.isFlushing.Store(false)
	nf.base.reset()
	nf.persistID = persistID

	baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
	baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))
	nodeBufferPersistID.Mark(int64(nf.persistID))
	return nil
}

// setSize sets the buffer size to the provided number, and invokes a flush
// operation if the current memory usage exceeds the new limit.
func (nf *nodebufferlist) setSize(size int, db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64) error {
	return errors.New("not supported")
}

// reset cleans up the disk cache.
func (nf *nodebufferlist) reset() {
	nf.mux.Lock()
	defer nf.mux.Unlock()

	for {
		nc := nf.popBack()
		if nc == nil {
			break
		}
		nodeCachePool.Put(nc)
	}
	buffer := nodeCachePool.Get().(*nodecache)
	buffer.reset()
	nf.head = buffer
	nf.tail = buffer
	nf.base.reset()

	baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
	baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))
}

// empty returns an indicator if trienodebuffer contains any state transition inside
func (nf *nodebufferlist) empty() bool {
	return nf.getLayers() == 0
}

// getSize return the trienodebuffer used size.
func (nf *nodebufferlist) getSize() (uint64, uint64) {
	// no lock, the return vals are used to log, not strictly correct
	return nf.size, nf.base.size
}

// getAllNodes return all the trie nodes are cached in trienodebuffer.
func (nf *nodebufferlist) getAllNodes() map[common.Hash]map[string]*trienode.Node {
	nf.mux.Lock()
	nf.basemMux.Lock()
	defer nf.mux.Unlock()
	defer nf.basemMux.Unlock()

	nc := nodeCachePool.Get().(*nodecache)
	nc.reset()
	if err := nc.commit(nf.base.block, nf.base.nodes, false); err != nil {
		log.Crit("failed to commit nodes to node buffer", "error", err)
	}
	merge := func(buffer *nodecache) bool {
		if err := nc.commit(buffer.block, buffer.nodes, false); err != nil {
			log.Crit("failed to commit nodes to node buffer", "error", err)
		}
		return true
	}
	nf.traverseReverse(merge)
	return nc.nodes
}

// getLayers return the size of cached difflayers.
func (nf *nodebufferlist) getLayers() uint64 {
	nf.mux.RLock()
	defer nf.mux.RUnlock()

	return nf.layers + nf.base.layers
}

// waitAndStopFlushing will block unit writing the trie nodes of trienodebuffer to disk.
func (nf *nodebufferlist) waitAndStopFlushing() {
	close(nf.stopCh)
	nf.stopFlushing.Store(true)
	for nf.isFlushing.Load() {
		time.Sleep(time.Second)
		log.Warn("waiting background node buffer flushed into disk")
	}
}

// setClean sets fastcache to trienodebuffer for cache the trie nodes, used for nodebufferlist.
func (nf *nodebufferlist) setClean(clean *fastcache.Cache) {
	nf.mux.Lock()
	defer nf.mux.Unlock()
	nf.clean = clean
}

// pushFront push cache to the nodebufferlist head.
func (nf *nodebufferlist) pushFront(cache *nodecache) {
	if cache == nil {
		return
	}
	cache.pre = nil
	cache.next = nf.head
	nf.head.pre = cache
	nf.head = cache

	nf.size += cache.size
	nf.layers += cache.layers
	nf.count++

	nodeBufferListSizeMeter.Mark(int64(nf.size))
	nodeBufferCountMeter.Mark(int64(nf.count))
	nodeBufferLayerMeter.Mark(int64(nf.layers))
	return
}

// pop the nodebufferlist tail element.
func (nf *nodebufferlist) popBack() *nodecache {
	if nf.tail == nil {
		return nil
	}
	tag := nf.tail
	nf.tail = nf.tail.pre
	if nf.tail != nil {
		nf.tail.next = nil
	}

	nf.size -= tag.size
	if nf.size < 0 {
		log.Warn("node buffer list size less 0", "old", nf.size, "dealt", tag.size)
		nf.size = 0
	}
	nf.layers -= tag.layers
	if nf.layers < 0 {
		log.Warn("node buffer list layers less 0", "old", nf.layers, "dealt", tag.layers)
		nf.layers = 0
	}
	nf.count--
	if nf.count < 0 {
		log.Warn("node buffer list count less 0", "old", nf.count)
		nf.count = 0
	}

	nodeBufferListSizeMeter.Mark(int64(nf.size))
	nodeBufferCountMeter.Mark(int64(nf.count))
	nodeBufferLayerMeter.Mark(int64(nf.layers))
	return tag
}

// traverse iterates the nodebufferlist and call the cb.
func (nf *nodebufferlist) traverse(cb func(*nodecache) bool) {
	cursor := nf.head
	for {
		if cursor == nil {
			return
		}
		next := cursor.next
		if !cb(cursor) {
			break
		}
		cursor = next
	}
	return
}

// traverseReverse iterates the nodebufferlist in reverse and call the cb.
func (nf *nodebufferlist) traverseReverse(cb func(*nodecache) bool) {
	cursor := nf.tail
	for {
		if cursor == nil {
			return
		}
		pre := cursor.pre
		if !cb(cursor) {
			break
		}
		cursor = pre
	}
	return
}

// diffToBase calls traverseReverse and merges the nodecache's nodes to
// base node buffer, if up to limit size and flush to disk. It is called
// periodically in the background
func (nf *nodebufferlist) diffToBase() {
	commitFunc := func(buffer *nodecache) bool {
		if nf.base.size >= nf.limit {
			log.Debug("base node buffer need write disk immediately")
			return false
		}
		if nf.count < reserveNodeCacheNumber {
			log.Debug("node buffer list less, waiting more difflayers are committed")
			return false
		}
		if buffer.block%mergeBlockInterval != 0 {
			log.Crit("committed block number misaligned", "block", buffer.block)
		}

		nf.basemMux.Lock()
		err := nf.base.commit(buffer.block, buffer.nodes, false)
		nf.basemMux.Unlock()
		if err != nil {
			log.Info("failed to commit nodes to base node buffer", "error", err)
		}
		del := nf.popBack()
		nf.base.layers--
		nf.base.layers += del.layers
		nodeCachePool.Put(del)

		baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
		baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))

		nf.report()
		return true
	}
	nf.traverseReverse(commitFunc)
}

// backgroundFlush flush base node buffer to disk.
func (nf *nodebufferlist) backgroundFlush() {
	nf.basemMux.RLock()
	persistID := nf.persistID + nf.base.layers
	nf.basemMux.RUnlock()
	err := nf.base.flush(nf.db, nf.clean, persistID, false)
	if err != nil {
		log.Crit("failed to flush base node buffer to disk", "error", err)
	}
	nf.basemMux.Lock()
	nf.base.reset()
	nf.persistID = persistID
	nf.basemMux.Unlock()
	baseNodeBufferSizeMeter.Mark(int64(nf.base.size))
	baseNodeBufferLayerMeter.Mark(int64(nf.base.layers))
	nodeBufferPersistID.Mark(int64(nf.persistID))
}

// loop runs the background task, collects the nodes for writing to disk.
func (nf *nodebufferlist) loop() {
	mergeTicker := time.NewTicker(time.Second * mergeNodeCacheInterval)
	for {
		select {
		case <-nf.stopCh:
			return
		case <-mergeTicker.C:
			nf.diffToBase()
			if nf.base.size > nf.limit {
				if nf.stopFlushing.Load() {
					nf.mux.Unlock()
					continue
				}
				if nf.isFlushing.Swap(true) {
					nf.mux.Unlock()
					continue
				}
				nf.backgroundFlush()
				nf.isFlushing.Swap(false)
			}
		}
	}
}

// report logs the nodebufferlist info for monitor.
func (nf *nodebufferlist) report() {
	log.Info("node buffer list info", "block_number", nf.block, "layers", nf.layers,
		"node_buffer_count", nf.count, "persist_id", nf.persistID, "list_size", common.StorageSize(nf.size),
		"base_buffer_size", common.StorageSize(nf.base.size), "base_buffer_layer", nf.base.layers)
}
