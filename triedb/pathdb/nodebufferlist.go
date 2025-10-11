package pathdb

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"golang.org/x/sync/errgroup"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

const (
	// mergeMultiDifflayerInterval defines the interval to collect nodes to flush disk.
	mergeMultiDifflayerInterval = 3

	// DefaultProposeBlockInterval defines the interval of op-proposer proposes block.
	DefaultProposeBlockInterval = 3600

	// DefaultReserveMultiDifflayerNumber defines the default reserve number of multiDifflayer in nodebufferlist.
	DefaultReserveMultiDifflayerNumber = 3
)

type KeepRecord struct {
	BlockID               uint64
	StateRoot             common.Hash
	KeepInterval          uint64
	PinnedInnerTrieReader layer
}
type NotifyKeepFunc func(*KeepRecord)

var _ trienodebuffer = &nodebufferlist{}

// nodebufferlist implements the trienodebuffer interface, it is designed to meet
// the withdraw proof function of opBNB at the storage layer while taking into
// account high performance. It is a multiDifflayer based queue that stores
// mergeBlockInterval compressed block difflayers per multiDifflayer. It also has
// one base multiDifflayer that collects the list's trie nodes to write disk.
type nodebufferlist struct {
	db        ethdb.Database   // Persistent storage for matured trie nodes.
	clean     *fastcache.Cache // GC friendly memory cache of clean node RLPs.
	wpBlocks  uint64           // Propose block to L1 block interval.
	rsevMdNum uint64           // Reserve number of multiDifflayer in nodebufferlist.
	dlInMd    uint64           // Difflayer number in multiDifflayer.

	limit   uint64          // The maximum memory allowance in bytes for base multiDifflayer.
	block   uint64          // Corresponding last update block number.
	stateId uint64          // Corresponding last update state id.
	size    uint64          // Size of nodebufferlist
	count   uint64          // Count of multiDifflayer in nodebufferlist
	layers  uint64          // Layers in nodebufferlist
	head    *multiDifflayer // The first element of nodebufferlist.
	tail    *multiDifflayer // The last element of nodebufferlist.
	mux     sync.RWMutex

	base      *multiDifflayer // Collect the nodes of nodebufferlist and write to disk.
	persistID uint64          // The last state id that have written to disk.
	baseMux   sync.RWMutex    // The mutex of base multiDifflayer and persistID.
	flushMux  sync.RWMutex    // The mutex of flushing base multiDifflayer for reorg corner case.

	useBase         atomic.Bool    // Flag if just use base buffer
	isFlushing      atomic.Bool    // Flag indicates writing disk under background.
	stopFlushing    atomic.Bool    // Flag stops writing disk under background.
	stopCh          chan struct{}  // Trigger stop background event loop.
	waitStopCh      chan struct{}  // Wait stop background event loop.
	forceKeepCh     chan struct{}  // Trigger force keep event loop.
	waitForceKeepCh chan struct{}  // Wait force keep event loop.
	keepFunc        NotifyKeepFunc // Used to keep op-proposer output proof.
}

// newNodeBufferList initializes the node buffer list with the provided nodes
func newNodeBufferList(
	db ethdb.Database,
	limit uint64,
	nodes map[common.Hash]map[string]*trienode.Node,
	layers uint64,
	proposeBlockInterval uint64,
	keepFunc NotifyKeepFunc,
	freezer *rawdb.ResettableFreezer,
	fastRecovery bool,
	useBase bool,
) (*nodebufferlist, error) {
	var (
		rsevMdNum uint64
		dlInMd    uint64
		wpBlocks  = proposeBlockInterval
	)
	if wpBlocks == 0 {
		rsevMdNum = DefaultReserveMultiDifflayerNumber
		wpBlocks = DefaultProposeBlockInterval
		dlInMd = DefaultProposeBlockInterval / (DefaultReserveMultiDifflayerNumber - 1)
	} else if wpBlocks%(DefaultReserveMultiDifflayerNumber-1) == 0 {
		rsevMdNum = DefaultReserveMultiDifflayerNumber
		dlInMd = wpBlocks / (DefaultReserveMultiDifflayerNumber - 1)
	} else {
		rsevMdNum = 1
		dlInMd = wpBlocks
	}

	if nodes == nil {
		nodes = make(map[common.Hash]map[string]*trienode.Node)
	}
	var size uint64
	for _, subset := range nodes {
		for path, n := range subset {
			size += uint64(len(n.Blob) + len(path))
		}
	}
	base := newMultiDifflayer(limit, size, common.Hash{}, nodes, layers)

	var (
		nf  *nodebufferlist
		err error
	)
	if !useBase && fastRecovery {
		nf, err = recoverNodeBufferList(db, freezer, base, limit, wpBlocks, rsevMdNum, dlInMd)
		if err != nil {
			log.Error("Failed to recover node buffer list", "error", err)
			return nil, err
		}
	} else {
		ele := newMultiDifflayer(limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
		nf = &nodebufferlist{
			db:              db,
			wpBlocks:        wpBlocks,
			rsevMdNum:       rsevMdNum,
			dlInMd:          dlInMd,
			limit:           limit,
			base:            base,
			head:            ele,
			tail:            ele,
			count:           1,
			persistID:       rawdb.ReadPersistentStateID(db),
			stopCh:          make(chan struct{}),
			waitStopCh:      make(chan struct{}),
			forceKeepCh:     make(chan struct{}),
			waitForceKeepCh: make(chan struct{}),
			keepFunc:        keepFunc,
		}
		nf.useBase.Store(useBase)
	}

	go nf.loop()

	log.Info("new node buffer list", "proposed block interval", nf.wpBlocks,
		"reserve multi difflayers", nf.rsevMdNum, "difflayers in multidifflayer", nf.dlInMd,
		"limit", common.StorageSize(limit), "layers", layers, "persist id", nf.persistID, "base_size", size)
	return nf, nil
}

// recoverNodeBufferList recovers node buffer list
func recoverNodeBufferList(db ethdb.Database, freezer *rawdb.ResettableFreezer, base *multiDifflayer,
	limit, wpBlocks, rsevMdNum, dlInMd uint64) (*nodebufferlist, error) {
	nbl := &nodebufferlist{
		db:              db,
		wpBlocks:        wpBlocks,
		rsevMdNum:       rsevMdNum,
		dlInMd:          dlInMd,
		limit:           limit,
		base:            base,
		persistID:       rawdb.ReadPersistentStateID(db),
		stopCh:          make(chan struct{}),
		waitStopCh:      make(chan struct{}),
		forceKeepCh:     make(chan struct{}),
		waitForceKeepCh: make(chan struct{}),
	}
	head, err := freezer.Ancients()
	if err != nil {
		log.Error("Failed to get freezer ancients", "error", err)
		return nil, err
	}
	tail, err := freezer.Tail()
	if err != nil {
		log.Error("Failed to get freezer tail", "error", err)
		return nil, err
	}
	log.Info("Ancient db meta info", "persistent_state_id", nbl.persistID, "head_state_id", head,
		"tail_state_id", tail, "waiting_recover_num", head-nbl.persistID)

	startStateID := nbl.persistID + 1
	startBlock, err := readBlockNumber(freezer, startStateID)
	if err != nil {
		log.Error("Failed to read start block number", "error", err, "tail_state_id", startStateID)
		return nil, err
	}
	endBlock, err := readBlockNumber(freezer, head)
	if err != nil {
		log.Error("Failed to read end block number", "error", err, "head_state_id", head)
		return nil, err
	}
	blockIntervals := nbl.createBlockInterval(startBlock, endBlock)
	stateIntervals, err := nbl.createStateInterval(freezer, startStateID, head, blockIntervals)
	if err != nil {
		return nil, err
	}
	log.Info("block intervals info", "blockIntervals", blockIntervals, "stateIntervals", stateIntervals,
		"startBlock", startBlock, "endBlock", endBlock)

	var eg errgroup.Group
	nbl.linkMultiDiffLayers(len(blockIntervals))
	for current, i := nbl.head, 0; current != nil; current, i = current.next, i+1 {
		index := i
		mdl := current
		eg.Go(func() error {
			for j := stateIntervals[index][0]; j <= stateIntervals[index][1]; j++ {
				h, err := nbl.readStateHistory(freezer, j)
				if err != nil {
					log.Error("Failed to read state history", "error", err)
					return err
				}
				if err = mdl.commit(h.meta.root, j, h.meta.block, 1, flattenTrieNodes(h.nodes)); err != nil {
					log.Error("Failed to commit trie nodes to multi diff layer", "error", err)
					return err
				}
			}
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	for current, i := nbl.head, 0; current != nil; current, i = current.next, i+1 {
		nbl.size += current.size
		nbl.layers += current.layers
	}
	nbl.diffToBase()

	log.Info("Succeed to add diff layer", "base_size", nbl.base.size, "tail_state_id", nbl.tail.id,
		"head_state_id", nbl.head.id, "nbl_layers", nbl.layers, "base_layers", nbl.base.layers)
	return nbl, nil
}

// linkMultiDiffLayers links specified amount of multiDiffLayers for recovering
func (nf *nodebufferlist) linkMultiDiffLayers(blockIntervalLength int) {
	for i := 0; i < blockIntervalLength; i++ {
		mdl := newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
		nf.pushFront(mdl)
	}
	nf.count = uint64(blockIntervalLength)
}

func (nf *nodebufferlist) readStateHistory(freezer *rawdb.ResettableFreezer, stateID uint64) (*history, error) {
	h, err := readHistory(freezer, stateID, true)
	if err != nil || h.nodes == nil {
		log.Error("Failed to read history from freezer db", "error", err)
		return nil, err
	}
	return h, nil
}

func (nf *nodebufferlist) createBlockInterval(startBlock, endBlock uint64) [][]uint64 {
	var intervalBoundaries [][]uint64
	firstIntervalEnd := startBlock + nf.dlInMd - (startBlock % nf.dlInMd)
	if endBlock < firstIntervalEnd {
		firstIntervalEnd = endBlock
	}
	intervalBoundaries = append(intervalBoundaries, []uint64{startBlock, firstIntervalEnd})

	for start := firstIntervalEnd + 1; start <= endBlock; start += nf.dlInMd {
		end := start + nf.dlInMd - 1
		if end > endBlock {
			end = endBlock
		}
		intervalBoundaries = append(intervalBoundaries, []uint64{start, end})
	}

	sort.Slice(intervalBoundaries, func(i, j int) bool {
		return intervalBoundaries[i][0] > intervalBoundaries[j][0]
	})
	return intervalBoundaries
}

func (nf *nodebufferlist) createStateInterval(freezer *rawdb.ResettableFreezer, startStateID, endStateID uint64,
	blockIntervals [][]uint64) ([][]uint64, error) {
	blockMap, err := readAllBlockNumbers(freezer, startStateID, endStateID)
	if err != nil {
		log.Crit("Failed to read all history meta", "error", err)
	}

	var stateIntervals [][]uint64
	for _, blockList := range blockIntervals {
		firstStateID, ok := blockMap[blockList[0]]
		if !ok {
			log.Error("Corresponding state id is not found", "block", blockList[0])
			return nil, fmt.Errorf("block %d is not found", blockList[0])
		}

		secondStateID, ok := blockMap[blockList[1]]
		if !ok {
			log.Error("Corresponding state id is not found", "block", blockList[1])
			return nil, fmt.Errorf("block %d is not found", blockList[1])
		}
		stateIntervals = append(stateIntervals, []uint64{firstStateID, secondStateID})
	}
	return stateIntervals, nil
}

func (nf *nodebufferlist) getLatestStatus() (common.Hash, uint64, error) {
	head := nf.head
	log.Info("last head multi diff layer info", "root", head.root, "id", head.id, "block", head.block,
		"layer", head.layers, "size", head.size)
	return head.root, head.id, nil
}

// node retrieves the trie node with given node info.
func (nf *nodebufferlist) node(owner common.Hash, path []byte, hash common.Hash) (node *trienode.Node, err error) {
	nf.mux.RLock()
	defer nf.mux.RUnlock()

	if nf.useBase.Load() {
		nf.baseMux.RLock()
		node, err = nf.base.node(owner, path, hash)
		nf.baseMux.RUnlock()
		return node, err
	}

	find := func(nc *multiDifflayer) bool {
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
		return nil, err
	}
	if node != nil {
		return node, nil
	}

	nf.baseMux.RLock()
	node, err = nf.base.node(owner, path, hash)
	nf.baseMux.RUnlock()
	return node, err
}

// commit merges the dirty nodes into the trienodebuffer. This operation won't take
// the ownership of the nodes map which belongs to the bottom-most diff layer.
// It will just hold the node references from the given map which are safe to
// copy.
func (nf *nodebufferlist) commit(root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node) trienodebuffer {
	nf.mux.Lock()
	defer nf.mux.Unlock()

	if nf.useBase.Load() {
		for {
			if nf.isFlushing.Swap(true) {
				time.Sleep(time.Duration(DefaultBackgroundFlushInterval) * time.Second)
				log.Info("waiting base node buffer to be flushed to disk")
				continue
			} else {
				break
			}
		}
		defer nf.isFlushing.Store(false)

		nf.baseMux.Lock()
		defer nf.baseMux.Unlock()
		if err := nf.base.commit(root, id, block, 1, nodes); err != nil {
			log.Crit("Failed to commit nodes to node buffer list", "error", err)
		}
		return nf
	}

	if nf.head == nil {
		nf.head = newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
		nf.tail = nf.head
	}
	oldSize := nf.head.size
	err := nf.head.commit(root, id, block, 1, nodes)
	if err != nil {
		log.Crit("failed to commit nodes to node buffer list", "error", err)
	}

	nf.stateId = id
	nf.block = block
	nf.size = nf.size + nf.head.size - oldSize
	nf.layers++

	nodeBufferListSizeGauge.Update(int64(nf.size))
	nodeBufferListLayerGauge.Update(int64(nf.layers))
	nodeBufferListLastStateIdGauge.Update(int64(nf.stateId))
	nodeBufferListLastBlockGauge.Update(int64(nf.block))

	if block != 0 && block%nf.dlInMd == 0 {
		nc := newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
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
	nf.baseMux.Lock()
	nf.flushMux.Lock()
	defer nf.mux.Unlock()
	defer nf.baseMux.Unlock()
	defer nf.flushMux.Unlock()

	merge := func(buffer *multiDifflayer) bool {
		if err := nf.base.commit(buffer.root, buffer.id, buffer.block, buffer.layers, buffer.nodes); err != nil {
			log.Crit("failed to commit nodes to base node buffer", "error", err)
		}
		_ = nf.popBack()
		return true
	}
	nf.traverseReverse(merge)
	nc := newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
	nf.head = nc
	nf.tail = nc
	nf.size = 0
	nf.layers = 0
	nf.count = 1
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

	nf.forceKeepCh <- struct{}{}
	<-nf.waitForceKeepCh

	// hang user read/write and background write
	nf.mux.Lock()
	nf.baseMux.Lock()
	nf.flushMux.Lock()
	defer nf.mux.Unlock()
	defer nf.baseMux.Unlock()
	defer nf.flushMux.Unlock()

	nf.stopFlushing.Store(true)
	defer nf.stopFlushing.Store(false)
	for {
		if nf.isFlushing.Swap(true) {
			time.Sleep(time.Duration(DefaultBackgroundFlushInterval) * time.Second)
			log.Info("waiting base node buffer to be flushed to disk")
			continue
		} else {
			break
		}
	}

	commitFunc := func(buffer *multiDifflayer) bool {
		if nf.count <= nf.rsevMdNum {
			log.Info("keep multiDiffLayer in node bufferList for getting withdrawal proof",
				"reserved_multidiflayer", nf.rsevMdNum, "bufferList_count", nf.count)
			return false
		}
		if err := nf.base.commit(buffer.root, buffer.id, buffer.block, buffer.layers, buffer.nodes); err != nil {
			log.Crit("failed to commit nodes to base node buffer", "error", err)
		}
		_ = nf.popBack()
		return true
	}
	nf.traverseReverse(commitFunc)

	persistID := nf.persistID + nf.base.layers
	err := nf.base.flush(nf.db, nf.clean, persistID)
	if err != nil {
		log.Crit("failed to flush base node buffer to disk", "error", err)
	}
	nf.isFlushing.Store(false)
	nf.base.reset()
	nf.persistID = persistID

	return nil
}

// setSize sets the buffer size to the provided number, and invokes a flush
// operation if the current memory usage exceeds the new limit.
func (nf *nodebufferlist) setSize(size int, db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64) error {
	return errors.New("node buffer list not supported")
}

// reset cleans up the disk cache.
func (nf *nodebufferlist) reset() {
	nf.mux.Lock()
	nf.baseMux.Lock()
	defer nf.mux.Unlock()
	defer nf.baseMux.Unlock()

	mf := newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
	nf.head = mf
	nf.tail = mf
	nf.size = 0
	nf.count = 1
	nf.layers = 0
	nf.base.reset()
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
	nf.baseMux.Lock()
	defer nf.mux.Unlock()
	defer nf.baseMux.Unlock()

	nc := newMultiDifflayer(nf.limit, 0, common.Hash{}, make(map[common.Hash]map[string]*trienode.Node), 0)
	if err := nc.commit(nf.base.root, nf.base.id, nf.base.block, nf.layers, nf.base.nodes); err != nil {
		log.Crit("failed to commit nodes to node buffer", "error", err)
	}
	merge := func(buffer *multiDifflayer) bool {
		if err := nc.commit(buffer.root, buffer.id, buffer.block, buffer.layers, buffer.nodes); err != nil {
			log.Crit("failed to commit nodes to node buffer", "error", err)
		}
		return true
	}
	nf.traverseReverse(merge)
	return nc.nodes
}

// getLayers return the size of cached difflayers.
func (nf *nodebufferlist) getLayers() uint64 {
	for {
		if nf.isFlushing.Swap(true) {
			time.Sleep(time.Duration(DefaultBackgroundFlushInterval) * time.Second)
			log.Info("waiting base node buffer to be flushed to disk")
			continue
		} else {
			break
		}
	}
	defer nf.isFlushing.Store(false)

	nf.mux.RLock()
	nf.baseMux.RLock()
	nf.flushMux.Lock()
	defer nf.mux.RUnlock()
	defer nf.baseMux.RUnlock()
	defer nf.flushMux.Unlock()

	return nf.layers + nf.base.layers
}

// waitAndStopFlushing will block unit writing the trie nodes of trienodebuffer to disk.
func (nf *nodebufferlist) waitAndStopFlushing() {
	close(nf.stopCh)
	<-nf.waitStopCh
	nf.stopFlushing.Store(true)
	for nf.isFlushing.Load() {
		time.Sleep(time.Second)
		log.Warn("waiting background node buffer to be flushed to disk")
	}
}

// setClean sets fastcache to trienodebuffer for cache the trie nodes, used for nodebufferlist.
func (nf *nodebufferlist) setClean(clean *fastcache.Cache) {
	nf.clean = clean
}

// pushFront push cache to the nodebufferlist head.
func (nf *nodebufferlist) pushFront(cache *multiDifflayer) {
	if cache == nil {
		return
	}
	if nf.head == nil {
		nf.head = cache
		nf.tail = cache
		cache.next = nil
		cache.pre = nil
		return
	}
	cache.pre = nil
	cache.next = nf.head
	nf.head.pre = cache
	nf.head = cache

	nf.size += cache.size
	nf.layers += cache.layers
	nf.count++

	return
}

// pop the nodebufferlist tail element.
func (nf *nodebufferlist) popBack() *multiDifflayer {
	if nf.tail == nil {
		return nil
	}
	if nf.head == nf.tail {
		nf.head = nil
		nf.tail = nil
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

	return tag
}

// traverse iterates the nodebufferlist and call the cb.
func (nf *nodebufferlist) traverse(cb func(*multiDifflayer) bool) {
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
func (nf *nodebufferlist) traverseReverse(cb func(*multiDifflayer) bool) {
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

// diffToBase calls traverseReverse and merges the multiDifflayer's nodes to
// base node buffer, if up to limit size and flush to disk. It is called
// periodically in the background
func (nf *nodebufferlist) diffToBase() {
	commitFunc := func(buffer *multiDifflayer) bool {
		if nf.base.size >= nf.base.limit {
			log.Debug("base node buffer need write disk immediately")
			return false
		}
		if nf.count <= nf.rsevMdNum {
			log.Debug("node buffer list less, waiting more difflayer to be committed")
			return false
		}
		if buffer.block%nf.dlInMd != 0 {
			log.Crit("committed block number misaligned", "block", buffer.block)
		}

		if nf.keepFunc != nil { // keep in background flush stage
			nf.keepFunc(&KeepRecord{
				BlockID:      buffer.block,
				StateRoot:    buffer.root,
				KeepInterval: nf.wpBlocks,
				PinnedInnerTrieReader: &proposedBlockReader{
					nf:   nf,
					diff: buffer,
				}})
		}

		nf.baseMux.Lock()
		err := nf.base.commit(buffer.root, buffer.id, buffer.block, buffer.layers, buffer.nodes)
		nf.baseMux.Unlock()
		if err != nil {
			log.Error("failed to commit nodes to base node buffer", "error", err)
			return false
		}

		nf.mux.Lock()
		_ = nf.popBack()
		nodeBufferListSizeGauge.Update(int64(nf.size))
		nodeBufferListCountGauge.Update(int64(nf.count))
		nodeBufferListLayerGauge.Update(int64(nf.layers))
		if nf.layers > 0 {
			nodeBufferListDifflayerAvgSize.Update(int64(nf.size / nf.layers))
		}
		nf.mux.Unlock()
		baseNodeBufferSizeGauge.Update(int64(nf.base.size))
		baseNodeBufferLayerGauge.Update(int64(nf.base.layers))
		if nf.base.layers > 0 {
			baseNodeBufferDifflayerAvgSize.Update(int64(nf.base.size / nf.base.layers))
		}
		nf.report()

		return true
	}
	nf.traverseReverse(commitFunc)
}

// backgroundFlush flush base node buffer to disk.
func (nf *nodebufferlist) backgroundFlush() {
	nf.flushMux.Lock()
	defer nf.flushMux.Unlock()
	nf.baseMux.RLock()
	persistID := nf.persistID + nf.base.layers
	nf.baseMux.RUnlock()
	err := nf.base.flush(nf.db, nf.clean, persistID)
	if err != nil {
		log.Error("failed to flush base node buffer to disk", "error", err)
		return
	}
	nf.baseMux.Lock()
	nf.base.reset()
	nf.persistID = persistID
	nf.baseMux.Unlock()

	baseNodeBufferSizeGauge.Update(int64(nf.base.size))
	baseNodeBufferLayerGauge.Update(int64(nf.base.layers))
	nodeBufferListPersistIDGauge.Update(int64(nf.persistID))
}

// loop runs the background task, collects the nodes for writing to disk.
func (nf *nodebufferlist) loop() {
	mergeTicker := time.NewTicker(time.Second * mergeMultiDifflayerInterval)
	defer mergeTicker.Stop()
	for {
		select {
		case <-nf.stopCh:
			if nf.keepFunc != nil { // keep in stop stage
				nf.mux.RLock()
				traverseKeepFunc := func(buffer *multiDifflayer) bool {
					nf.keepFunc(&KeepRecord{
						BlockID:      buffer.block,
						StateRoot:    buffer.root,
						KeepInterval: nf.wpBlocks,
						PinnedInnerTrieReader: &proposedBlockReader{
							nf:   nf,
							diff: buffer,
						}})
					return true
				}
				nf.traverseReverse(traverseKeepFunc)
				nf.mux.RUnlock()
			}
			nf.waitStopCh <- struct{}{}
			return

		case <-nf.forceKeepCh:
			if nf.keepFunc != nil { // keep in force flush stage
				nf.mux.RLock()
				traverseKeepFunc := func(buffer *multiDifflayer) bool {
					nf.keepFunc(&KeepRecord{
						BlockID:      buffer.block,
						StateRoot:    buffer.root,
						KeepInterval: nf.wpBlocks,
						PinnedInnerTrieReader: &proposedBlockReader{
							nf:   nf,
							diff: buffer,
						}})
					return true
				}
				nf.traverseReverse(traverseKeepFunc)
				nf.mux.RUnlock()
			}
			nf.waitForceKeepCh <- struct{}{}

		case <-mergeTicker.C:
			if nf.stopFlushing.Load() {
				continue
			}
			if nf.isFlushing.Swap(true) {
				continue
			}
			nf.diffToBase()
			if nf.base.size >= nf.base.limit {
				nf.backgroundFlush()
			}
			nf.isFlushing.Swap(false)
		}
	}
}

// proposedBlockReader return the world state Reader of block that is proposed to L1.
func (nf *nodebufferlist) proposedBlockReader(blockRoot common.Hash) (layer, error) {
	nf.mux.RLock()
	defer nf.mux.RUnlock()

	var diff *multiDifflayer
	context := []interface{}{
		"root", blockRoot,
	}
	find := func(buffer *multiDifflayer) bool {
		context = append(context, []interface{}{"multi_difflayer_number", buffer.block}...)
		context = append(context, []interface{}{"multi_difflayer_root", buffer.root}...)
		if buffer.block%nf.wpBlocks == 0 {
			if buffer.root == blockRoot {
				diff = buffer
				return false
			}
		}
		return true
	}
	nf.traverse(find)
	if diff == nil {
		proposedBlockReaderMismatch.Mark(1)
		log.Debug("proposed block state is not available", context...)
		return nil, fmt.Errorf("proposed block proof state %#x is not available", blockRoot)
	}
	proposedBlockReaderSuccess.Mark(1)
	return &proposedBlockReader{
		nf:   nf,
		diff: diff,
	}, nil
}

// report logs the nodebufferlist info for monitor.
func (nf *nodebufferlist) report() {
	context := []interface{}{
		"number", nf.block, "count", nf.count, "layers", nf.layers,
		"stateid", nf.stateId, "persist", nf.persistID, "size", common.StorageSize(nf.size),
		"basesize", common.StorageSize(nf.base.size), "baselayers", nf.base.layers,
	}
	log.Info("node buffer list info", context...)
}

var _ layer = &proposedBlockReader{}

// proposedBlockReader implements the layer interface used to read the status of proposed
// blocks, which supports get withdrawal proof. It only needs to implement the Node function
// of the Reader interface.
type proposedBlockReader struct {
	nf   *nodebufferlist
	diff *multiDifflayer
}

func (w *proposedBlockReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	w.nf.mux.RLock()
	defer w.nf.mux.RUnlock()

	current := w.diff
	for {
		if current == nil {
			break
		}
		node, err := current.node(owner, path, hash)
		if err != nil {
			return nil, err
		}
		if node != nil {
			return node.Blob, nil
		}
		current = current.next
	}

	w.nf.baseMux.RLock()
	node, err := w.nf.base.node(owner, path, hash)
	w.nf.baseMux.RUnlock()
	if err != nil {
		return nil, err
	}
	if node != nil {
		return node.Blob, nil
	}

	key := cacheKey(owner, path)
	if w.nf.clean != nil {
		if blob := w.nf.clean.Get(nil, key); len(blob) > 0 {
			h := newHasher()
			defer h.release()

			got := h.hash(blob)
			if got == hash {
				cleanHitMeter.Mark(1)
				cleanReadMeter.Mark(int64(len(blob)))
				return blob, nil
			}
			cleanFalseMeter.Mark(1)
			log.Error("Unexpected trie node in clean cache", "owner", owner, "path", path, "expect", hash, "got", got)
		}
		cleanMissMeter.Mark(1)
	}

	var (
		nBlob []byte
		nHash common.Hash
	)
	if owner == (common.Hash{}) {
		nBlob, nHash = rawdb.ReadAccountTrieNode(w.nf.db, path)
	} else {
		nBlob, nHash = rawdb.ReadStorageTrieNode(w.nf.db, owner, path)
	}
	if nHash != hash {
		diskFalseMeter.Mark(1)
		log.Error("Unexpected trie node in disk", "owner", owner, "path", path, "expect", hash, "got", nHash)
		return nil, newUnexpectedNodeError("disk", hash, nHash, owner, path, nBlob)
	}
	if w.nf.clean != nil && len(nBlob) > 0 {
		w.nf.clean.Set(key, nBlob)
		cleanWriteMeter.Mark(int64(len(nBlob)))
	}
	return nBlob, nil
}
func (w *proposedBlockReader) rootHash() common.Hash { return w.diff.root }
func (w *proposedBlockReader) stateID() uint64 {
	return w.diff.id
}
func (w *proposedBlockReader) parentLayer() layer { return nil }
func (w *proposedBlockReader) update(root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return nil
}
func (w *proposedBlockReader) journal(io.Writer, JournalType) error { return nil }

// multiDifflayer compresses several difflayers in one map. As an element of nodebufferlist
// it is the smallest unit for storing trie nodes.
type multiDifflayer struct {
	root   common.Hash                               // Corresponding last root hash to which this layer diff belongs to
	id     uint64                                    // Corresponding last update state id
	block  uint64                                    // Corresponding last update block number
	layers uint64                                    // The number of diff layers aggregated inside
	size   uint64                                    // The size of aggregated writes
	limit  uint64                                    // The maximum memory allowance in bytes
	nodes  map[common.Hash]map[string]*trienode.Node // The dirty node set, mapped by owner and path

	pre  *multiDifflayer
	next *multiDifflayer
}

// newMultiDifflayer initializes the multiDifflayer with the provided nodes
func newMultiDifflayer(limit, size uint64, root common.Hash, nodes map[common.Hash]map[string]*trienode.Node, layers uint64) *multiDifflayer {
	return &multiDifflayer{
		root:   root,
		layers: layers,
		size:   size,
		limit:  limit,
		nodes:  nodes,
	}
}

// node retrieves the trie node with given node info.
func (mf *multiDifflayer) node(owner common.Hash, path []byte, hash common.Hash) (*trienode.Node, error) {
	subset, ok := mf.nodes[owner]
	if !ok {
		return nil, nil
	}
	n, ok := subset[string(path)]
	if !ok {
		return nil, nil
	}
	if n.Hash != hash {
		dirtyFalseMeter.Mark(1)
		log.Error("Unexpected trie node in async node buffer", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
		return nil, newUnexpectedNodeError("dirty", hash, n.Hash, owner, path, n.Blob)
	}
	return n, nil
}

// commit merges the dirty nodes into the newMultiDifflayer. This operation won't
// take the ownership of the nodes map which belongs to the bottom-most diff layer.
// It will just hold the node references from the given map which are safe to copy.
func (mf *multiDifflayer) commit(root common.Hash, id uint64, block uint64, layers uint64, nodes map[common.Hash]map[string]*trienode.Node) error {
	if mf.id != 0 && mf.id >= id {
		log.Warn("state id out of order", "pre_stateId", mf.id, "capping_stateId", id)
	}
	if mf.block != 0 && mf.block >= block {
		log.Warn("block number out of order", "pre_block", mf.block, "capping_block", block)
	}

	mf.root = root
	mf.block = block
	mf.id = id

	var (
		delta         int64
		overwrite     int64
		overwriteSize int64
	)
	for owner, subset := range nodes {
		current, exist := mf.nodes[owner]
		if !exist {
			// Allocate a new map for the subset instead of claiming it directly
			// from the passed map to avoid potential concurrent map read/write.
			// The nodes belong to original diff layer are still accessible even
			// after merging, thus the ownership of nodes map should still belong
			// to original layer and any mutation on it should be prevented.
			current = make(map[string]*trienode.Node)
			for path, n := range subset {
				current[path] = n
				delta += int64(len(n.Blob) + len(path))
			}
			mf.nodes[owner] = current
			continue
		}
		for path, n := range subset {
			if orig, exist := current[path]; !exist {
				delta += int64(len(n.Blob) + len(path))
			} else {
				delta += int64(len(n.Blob) - len(orig.Blob))
				overwrite++
				overwriteSize += int64(len(orig.Blob) + len(path))
			}
			current[path] = n
		}
		mf.nodes[owner] = current
	}
	mf.updateSize(delta)
	mf.layers += layers
	gcNodesMeter.Mark(overwrite)
	gcBytesMeter.Mark(overwriteSize)
	return nil
}

// updateSize updates the size of newMultiDifflayer.
func (mf *multiDifflayer) updateSize(delta int64) {
	size := int64(mf.size) + delta
	if size >= 0 {
		mf.size = uint64(size)
		return
	}
	s := mf.size
	mf.size = 0
	log.Warn("Invalid pathdb buffer size", "prev", common.StorageSize(s), "delta", common.StorageSize(delta))
}

// reset clears the newMultiDifflayer.
func (mf *multiDifflayer) reset() {
	mf.root = common.Hash{}
	mf.id = 0
	mf.block = 0
	mf.layers = 0
	mf.size = 0
	mf.pre = nil
	mf.next = nil
	mf.nodes = make(map[common.Hash]map[string]*trienode.Node)
}

// empty returns an indicator if multiDifflayer contains any state transition inside.
func (mf *multiDifflayer) empty() bool {
	return mf.layers == 0
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (mf *multiDifflayer) flush(db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64) error {
	// Ensure the target state id is aligned with the internal counter.
	head := rawdb.ReadPersistentStateID(db)
	if head+mf.layers != id {
		return fmt.Errorf("buffer layers (%d) cannot be applied on top of persisted state id (%d) to reach requested state id (%d)", mf.layers, head, id)
	}
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(int(float64(mf.size) * DefaultBatchRedundancyRate))
	)
	nodes := writeNodes(batch, mf.nodes, clean)
	rawdb.WritePersistentStateID(batch, id)

	// Flush all mutations in a single batch
	size := batch.ValueSize()
	if err := batch.Write(); err != nil {
		return err
	}
	commitBytesMeter.Mark(int64(size))
	commitNodesMeter.Mark(int64(nodes))
	commitTimeTimer.UpdateSince(start)
	log.Info("Persisted pathdb nodes", "nodes", len(mf.nodes), "bytes", common.StorageSize(size), "state_id", id, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// revert is the reverse operation of commit. It also merges the provided nodes
// into the multiDifflayer, the difference is that the provided node set should
// revert the changes made by the last state transition.
func (mf *multiDifflayer) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) error {
	// Short circuit if no embedded state transition to revert.
	if mf.layers == 0 {
		return errStateUnrecoverable
	}
	mf.layers--

	// Reset the entire buffer if only a single transition left.
	if mf.layers == 0 {
		mf.reset()
		return nil
	}
	var delta int64
	for owner, subset := range nodes {
		current, ok := mf.nodes[owner]
		if !ok {
			panic(fmt.Sprintf("non-existent subset (%x)", owner))
		}
		for path, n := range subset {
			orig, ok := current[path]
			if !ok {
				// There is a special case in MPT that one child is removed from
				// a fullNode which only has two children, and then a new child
				// with different position is immediately inserted into the fullNode.
				// In this case, the clean child of the fullNode will also be
				// marked as dirty because of node collapse and expansion.
				//
				// In case of database rollback, don't panic if this "clean"
				// node occurs which is not present in buffer.
				var nhash common.Hash
				if owner == (common.Hash{}) {
					_, nhash = rawdb.ReadAccountTrieNode(db, []byte(path))
				} else {
					_, nhash = rawdb.ReadStorageTrieNode(db, owner, []byte(path))
				}
				// Ignore the clean node in the case described above.
				if nhash == n.Hash {
					continue
				}
				panic(fmt.Sprintf("non-existent node (%x %v) blob: %v", owner, path, crypto.Keccak256Hash(n.Blob).Hex()))
			}
			current[path] = n
			delta += int64(len(n.Blob)) - int64(len(orig.Blob))
		}
	}
	mf.updateSize(delta)
	return nil
}
