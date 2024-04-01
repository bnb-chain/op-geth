package pathdb

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"

	pebbledb "github.com/cockroachdb/pebble"
)

const (
	// DefaultMaxCheckpointNumber is used to keep checkpoint number, default is 3 days for main-net.
	DefaultMaxCheckpointNumber = 24 * 3

	// DefaultCheckpointDir is used to store checkpoint directory.
	DefaultCheckpointDir = "checkpoint"

	// gcCheckpointIntervalSecond is used to control gc loop interval.
	gcCheckpointIntervalSecond = 1

	// printCheckpointStatIntervalSecond is used to print checkpoint stat.
	printCheckpointStatIntervalSecond = 60

	// checkDBKeepaliveSecond is used to check db which has been not used for a long time,
	// and close it for reducing memory.
	checkDBKeepaliveIntervalSecond = 60

	// checkDBOpenedIntervalSecond is used to max opened db number to avoid too many db oom.
	checkDBOpenedIntervalSecond = 1

	// defaultMaxOpenedNumber is max opened number.
	defaultMaxOpenedNumber = 10
)

// encodeSubCheckpointDir encodes a completed checkpoint directory.
// eg: /datadir/geth/checkpoint/3653040_0xbe4838f06a7e85e85492e7a06ec3e838cc141b0b1b30fb4efe11f48b5f4e9f0e.
func encodeSubCheckpointDir(checkpointDir string, blockNumber uint64, blockRoot common.Hash) string {
	return checkpointDir + "/" + strconv.FormatUint(blockNumber, 10) + "_" + blockRoot.String()
}

// decodeSubCheckpointDir decodes meta information such as blockid and blockroot.
// eg: /datadir/geth/checkpoint/3653040_0xbe4838f06a7e85e85492e7a06ec3e838cc141b0b1b30fb4efe11f48b5f4e9f0e
// decode to blocknumber=3653040 and blockroot=0xbe4838f06a7e85e85492e7a06ec3e838cc141b0b1b30fb4efe11f48b5f4e9f0e.
func decodeSubCheckpointDir(subCheckpointDir string) (blockNumber uint64, blockRoot common.Hash, err error) {
	baseCheckpointDir := filepath.Base(subCheckpointDir)
	fields := strings.Split(baseCheckpointDir, "_")
	if len(fields) != 2 {
		return 0, common.Hash{}, fmt.Errorf("invalid fields counter, dir %v", baseCheckpointDir)
	}
	if blockNumber, err = strconv.ParseUint(fields[0], 10, 64); err != nil {
		return 0, common.Hash{}, fmt.Errorf("invalid block number, dir %v", baseCheckpointDir)
	}
	blockRoot = common.HexToHash(fields[1])
	return blockNumber, blockRoot, nil
}

// checkpointLayer only provides node interface.
type checkpointLayer struct {
	cm                   *checkpointManager
	blockNumber          uint64
	root                 common.Hash
	checkpointDir        string
	checkpointDB         ethdb.Database
	isOpened             atomic.Bool // lazy to open db when it is used.
	lastUsedTimestamp    time.Time   // db will be closed if it is not used for a long time.
	tryOpenCh            chan struct{}
	waitOpenCh           chan struct{}
	tryCloseAndDeleteCh  chan struct{}
	waitCloseAndDeleteCh chan struct{}
	tryCloseAndStopCh    chan struct{}
	waitCloseAndStopCh   chan struct{}
	tryCloseCh           chan struct{}
}

var _ layer = &checkpointLayer{}

// newCheckpointLayer is used to return make a checkpoint instance.
func newCheckpointLayer(cm *checkpointManager, checkpointDir string) (ckptLayer *checkpointLayer, err error) {
	var (
		blockNumber uint64
		blockRoot   common.Hash
	)

	if blockNumber, blockRoot, err = decodeSubCheckpointDir(checkpointDir); err != nil {
		log.Warn("Failed to decode checkpoint dir", "dir", checkpointDir, "error", err)
		return nil, err
	}
	ckptLayer = &checkpointLayer{
		cm:                cm,
		checkpointDir:     checkpointDir,
		blockNumber:       blockNumber,
		root:              blockRoot,
		lastUsedTimestamp: time.Now(),
	}
	ckptLayer.isOpened.Store(false)
	ckptLayer.tryOpenCh = make(chan struct{})
	ckptLayer.waitOpenCh = make(chan struct{})
	ckptLayer.tryCloseAndDeleteCh = make(chan struct{})
	ckptLayer.waitCloseAndDeleteCh = make(chan struct{})
	ckptLayer.tryCloseAndStopCh = make(chan struct{})
	ckptLayer.waitCloseAndStopCh = make(chan struct{})
	ckptLayer.tryCloseCh = make(chan struct{})
	go ckptLayer.loop()
	return ckptLayer, nil
}

// loop runs an event loop.
func (c *checkpointLayer) loop() {
	var (
		err                            error
		kvDB                           ethdb.KeyValueStore
		startOpenCheckpointTimestamp   time.Time
		endOpenCheckpointTimestamp     time.Time
		startCloseCheckpointTimestamp  time.Time
		endCloseCheckpointTimestamp    time.Time
		startDeleteCheckpointTimestamp time.Time
		endDeleteCheckpointTimestamp   time.Time
	)

	tryCloseTicker := time.NewTicker(time.Second * checkDBKeepaliveIntervalSecond)
	defer tryCloseTicker.Stop()

	for {
		select {
		case <-tryCloseTicker.C: // close db when a long time unused for reducing memory usage.
			if c.isOpened.Load() && time.Now().Sub(c.lastUsedTimestamp) > time.Second*checkDBKeepaliveIntervalSecond {
				startCloseCheckpointTimestamp = time.Now()
				if err = c.checkpointDB.Close(); err != nil {
					log.Warn("Failed to close checkpoint db", "error", err)
				} else {
					c.isOpened.Store(false)
				}
				endCloseCheckpointTimestamp = time.Now()
				closeCheckpointTimer.Update(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp))
			}
		case <-c.tryCloseCh: // close db when total opened db > max for reducing memory usage.
			if c.isOpened.Load() {
				startCloseCheckpointTimestamp = time.Now()
				if err = c.checkpointDB.Close(); err != nil {
					log.Warn("Failed to close checkpoint db", "error", err)
				} else {
					c.isOpened.Store(false)
				}
				endCloseCheckpointTimestamp = time.Now()
				closeCheckpointTimer.Update(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp))
			}
		case <-c.tryOpenCh: // lazy to open db for reducing memory usage.
			c.lastUsedTimestamp = time.Now()
			if !c.isOpened.Load() {
				startOpenCheckpointTimestamp = time.Now()
				kvDB, err = pebble.OpenCheckpointDB(c.checkpointDir, c.cm.sharedBlockCache, c.cm.sharedTableCache)
				if err == nil {
					c.checkpointDB = rawdb.NewDatabase(kvDB)
					c.isOpened.Store(true)
				} else {
					log.Warn("Failed to open checkpoint kv db", "error", err)
				}
				endOpenCheckpointTimestamp = time.Now()
				openCheckpointTimer.Update(endOpenCheckpointTimestamp.Sub(startOpenCheckpointTimestamp))
			}
			c.waitOpenCh <- struct{}{}
		case <-c.tryCloseAndDeleteCh: // gc for reducing disk usage.
			if c.isOpened.Load() {
				startCloseCheckpointTimestamp = time.Now()
				err = c.checkpointDB.Close()
				if err != nil {
					log.Warn("Failed to close checkpoint db", "error", err)
				} else {
					c.isOpened.Store(false)
				}
				endCloseCheckpointTimestamp = time.Now()
				log.Info("Close checkpoint db", "error", err,
					"elapsed", common.PrettyDuration(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp)))
				closeCheckpointTimer.Update(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp))
			}
			if !c.isOpened.Load() {
				startDeleteCheckpointTimestamp = time.Now()
				err = os.RemoveAll(c.checkpointDir)
				endDeleteCheckpointTimestamp = time.Now()
				log.Info("Delete checkpoint db", "error", err,
					"elapsed", common.PrettyDuration(endDeleteCheckpointTimestamp.Sub(startDeleteCheckpointTimestamp)),
					"checkpoint_dir", c.checkpointDir)
				deleteCheckpointTimer.Update(endDeleteCheckpointTimestamp.Sub(startDeleteCheckpointTimestamp))
			}
			c.waitCloseAndDeleteCh <- struct{}{}
		case <-c.tryCloseAndStopCh: // close for stop.
			if c.isOpened.Load() {
				startCloseCheckpointTimestamp = time.Now()
				err = c.checkpointDB.Close() // ignore this error.
				endCloseCheckpointTimestamp = time.Now()
				log.Info("Close checkpoint db", "error", err, "checkpoint_dir", c.checkpointDir,
					"elapsed", common.PrettyDuration(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp)))
				closeCheckpointTimer.Update(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp))
			}
			close(c.waitCloseAndStopCh)
			return
		}
	}
}

// waitDBOpen opens the db when the db is not open.
// Note that this is a best-effort operation and the db may still fail to open.
func (c *checkpointLayer) waitDBOpen() {
	c.tryOpenCh <- struct{}{}
	<-c.waitOpenCh
}

// waitDBCloseAndDelete closes the db and deletes the checkpoint directory when GC checkpoint.
// Note that this is a best-effort operation and the db may still fail to close and delete.
func (c *checkpointLayer) waitDBCloseAndDelete() {
	c.tryCloseAndDeleteCh <- struct{}{}
	<-c.waitCloseAndDeleteCh
}

// waitDBCloseAndStop closes the db and finish the background loop.
func (c *checkpointLayer) waitDBCloseAndStop() {
	c.tryCloseAndStopCh <- struct{}{}
	<-c.waitCloseAndStopCh
}

// closeDB closes the db.
func (c *checkpointLayer) closeDB() {
	c.tryCloseCh <- struct{}{}
}

// Node implements the layer node interface.
func (c *checkpointLayer) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	var (
		startTimestamp time.Time
		nBlob          []byte
		nHash          common.Hash
	)

	startTimestamp = time.Now()
	defer func() {
		queryCheckpointTimer.UpdateSince(startTimestamp)
	}()

	c.waitDBOpen()
	if !c.isOpened.Load() {
		return nil, errors.New("checkpoint db is not opened")
	}

	if owner == (common.Hash{}) {
		nBlob, nHash = rawdb.ReadAccountTrieNode(c.checkpointDB, path)
	} else {
		nBlob, nHash = rawdb.ReadStorageTrieNode(c.checkpointDB, owner, path)
	}
	if nHash != hash { // maybe due to the checkpointDB is closed in gc loop.
		diskFalseMeter.Mark(1)
		log.Error("Unexpected trie node in disk", "owner", owner, "path", path, "expect", hash, "got", nHash, "checkpoint_block_number", c.blockNumber)
		return nil, newUnexpectedNodeError("disk", hash, nHash, owner, path, nBlob)
	}
	return nBlob, nil
}

// rootHash implements the layer interface, returning the root hash of
// corresponding state.
func (c *checkpointLayer) rootHash() common.Hash {
	return c.root
}

// stateID implements the layer interface, note that this is unused.
func (c *checkpointLayer) stateID() uint64 {
	return 0
}

// parentLayer implements the layer interface, note that this is unused.
func (c *checkpointLayer) parentLayer() layer {
	return nil
}

// update implements the layer interface, note that this is unused.
func (c *checkpointLayer) update(common.Hash, uint64, uint64, map[common.Hash]map[string]*trienode.Node, *triestate.Set) *diffLayer {
	return nil
}

// journal implements the layer interface, note that this is unused.
func (c *checkpointLayer) journal(io.Writer) error {
	return nil
}

// checkpointManager struct is mainly manager checkpoint instance lifecycle.
type checkpointManager struct {
	db                      ethdb.Database
	enableCheckPoint        atomic.Bool
	checkpointDir           string
	checkpointBlockInterval uint64
	maxCheckpointNumber     uint64
	mux                     sync.RWMutex
	checkpointMap           map[common.Hash]*checkpointLayer
	stopCh                  chan struct{}
	waitCloseCh             chan struct{}
	sharedBlockCache        *pebbledb.Cache
	sharedTableCache        *pebbledb.TableCache
}

// newCheckpointManager returns a checkpoint manager instance.
func newCheckpointManager(db ethdb.Database, checkpointDir string, enableCheckPoint bool, checkpointBlockInterval uint64, maxCheckpointNumber uint64) *checkpointManager {
	startTimestamp := time.Now()
	if checkpointBlockInterval == 0 {
		checkpointBlockInterval = DefaultMaxCheckpointNumber
	}
	cm := &checkpointManager{db: db, checkpointDir: checkpointDir, checkpointBlockInterval: checkpointBlockInterval, maxCheckpointNumber: maxCheckpointNumber}
	cm.enableCheckPoint.Store(enableCheckPoint)
	cm.checkpointMap = make(map[common.Hash]*checkpointLayer)
	cm.stopCh = make(chan struct{})
	cm.waitCloseCh = make(chan struct{})
	cm.loadCheckpoint()

	cm.sharedBlockCache = pebbledb.NewCache(8 * 1024 * 1024) // 8MB
	cm.sharedTableCache = pebbledb.NewTableCache(cm.sharedBlockCache, runtime.NumCPU(), 4096)
	go cm.loop()
	cm.logDetailedInfo()
	log.Info("Succeed to new checkpoint manager", "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	return cm
}

// logDetailedInfo prints checkpoint manager detailed info.
func (cm *checkpointManager) logDetailedInfo() {
	var infos []interface{}
	infos = append(infos, []interface{}{"checkpoint_dir", cm.checkpointDir}...)
	infos = append(infos, []interface{}{"enable_checkpoint", cm.enableCheckPoint.Load()}...)
	infos = append(infos, []interface{}{"checkpoint_block_interval", cm.checkpointBlockInterval}...)
	infos = append(infos, []interface{}{"max_checkpoint_number", cm.maxCheckpointNumber}...)
	// the map may be modified concurrently, but it doesn't matter.
	infos = append(infos, []interface{}{"current_checkpoint_number", len(cm.checkpointMap)}...)
	log.Info("Checkpoint manager detailed info", infos...)
}

// close is used to close all checkpoint db and exit.
func (cm *checkpointManager) close() {
	startTimestamp := time.Now()
	close(cm.stopCh)
	<-cm.waitCloseCh
	cm.mux.Lock()
	defer cm.mux.Unlock()
	for _, ckptLayer := range cm.checkpointMap {
		ckptLayer.waitDBCloseAndStop()
	}
	cm.checkpointMap = make(map[common.Hash]*checkpointLayer)
	log.Info("Succeed to close checkpoint manager", "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
}

// needDoCheckpoint determines whether checkpoint needs to be done.
func (cm *checkpointManager) needDoCheckpoint(currentCommitBlockNumber uint64) bool {
	if !cm.enableCheckPoint.Load() || cm.checkpointBlockInterval == 0 || currentCommitBlockNumber == 0 {
		return false
	}
	if currentCommitBlockNumber%cm.checkpointBlockInterval == 0 {
		return true
	}
	return false
}

// loop runs an event loop.
func (cm *checkpointManager) loop() {
	gcCheckpointTicker := time.NewTicker(time.Second * gcCheckpointIntervalSecond)
	defer gcCheckpointTicker.Stop()
	printCheckpointStatTicker := time.NewTicker(time.Second * printCheckpointStatIntervalSecond)
	defer printCheckpointStatTicker.Stop()
	checkMaxDBOpenedTicker := time.NewTicker(time.Second * checkDBOpenedIntervalSecond)
	defer checkMaxDBOpenedTicker.Stop()

	for {
		select {
		case <-cm.stopCh:
			close(cm.waitCloseCh)
			return
		case <-gcCheckpointTicker.C:
			cm.gcCheckpoint()
		case <-printCheckpointStatTicker.C:
			cm.printCheckpointStat()
		case <-checkMaxDBOpenedTicker.C:
			cm.checkMaxDBOpened()
		}
	}
}

// checkMaxDBOpened closes the earliest opened db when the number of open dbs is greater than the configuration.
func (cm *checkpointManager) checkMaxDBOpened() {
	if !cm.enableCheckPoint.Load() {
		return
	}

	var (
		totalOpenedNumber int64
		toCloseCkptLayer  *checkpointLayer
	)
	cm.mux.RLock()
	for _, ckptLayer := range cm.checkpointMap {
		if ckptLayer.isOpened.Load() {
			totalOpenedNumber = totalOpenedNumber + 1
			if toCloseCkptLayer == nil || ckptLayer.lastUsedTimestamp.Before(toCloseCkptLayer.lastUsedTimestamp) {
				toCloseCkptLayer = ckptLayer
			}
		}
	}
	cm.mux.RUnlock()

	openedCheckpointSizeGauge.Update(totalOpenedNumber)
	if totalOpenedNumber > defaultMaxOpenedNumber {
		toCloseCkptLayer.closeDB()
	}
}

// gcCheckpoint deletes redundant checkpoints in the background.
func (cm *checkpointManager) gcCheckpoint() {
	if !cm.enableCheckPoint.Load() {
		return
	}
	if len(cm.checkpointMap) <= int(cm.maxCheckpointNumber) {
		return
	}

	var (
		gcBlockNumber uint64
		gcCkptLayer   *checkpointLayer
	)
	gcBlockNumber = math.MaxUint64

	cm.mux.RLock()
	for _, ckptLayer := range cm.checkpointMap {
		if ckptLayer.blockNumber < gcBlockNumber {
			gcBlockNumber = ckptLayer.blockNumber
			gcCkptLayer = ckptLayer
		}
	}
	cm.mux.RUnlock()

	if gcCkptLayer != nil {
		var (
			err            error
			startTimestamp time.Time
		)
		startTimestamp = time.Now()
		defer func() {
			gcCheckpointTimer.UpdateSince(startTimestamp)
			log.Info("GC one checkpoint", "checkpoint_block_number", gcCkptLayer.blockNumber, "error", err,
				"elapsed", common.PrettyDuration(time.Since(startTimestamp)))
		}()

		cm.mux.Lock()
		delete(cm.checkpointMap, gcCkptLayer.rootHash())
		cm.mux.Unlock()
		gcCkptLayer.waitDBCloseAndDelete()
	}
}

// printCheckpointStat is used to print checkpoint stat.
func (cm *checkpointManager) printCheckpointStat() error {
	if !cm.enableCheckPoint.Load() {
		return nil
	}

	cm.logDetailedInfo()
	cm.mux.RLock()
	checkpointSizeGauge.Update(int64(len(cm.checkpointMap)))
	for _, ckptLayer := range cm.checkpointMap {
		log.Info("Checkpoint detailed info",
			"checkpoint_block_number", ckptLayer.blockNumber,
			"checkpoint_block_root", ckptLayer.root.String(),
			"checkpoint_is_opened", ckptLayer.isOpened.Load(),
			"last_used_timestamp", ckptLayer.lastUsedTimestamp.String())
	}
	cm.mux.RUnlock()
	return nil
}

// loadCheckpoint loads all checkpoints on the disk at startup.
func (cm *checkpointManager) loadCheckpoint() error {
	if !cm.enableCheckPoint.Load() {
		return nil
	}

	var (
		err               error
		startTimestamp    time.Time
		ckptLayer         *checkpointLayer
		succeedLoadNumber uint64
	)

	startTimestamp = time.Now()
	defer func() {
		addCheckpointTimer.UpdateSince(startTimestamp)
		log.Info("Load checkpoint", "load_checkpoint_number", succeedLoadNumber,
			"error", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	}()

	entries, err := os.ReadDir(cm.checkpointDir)
	if err != nil {
		log.Warn("Failed to scan checkpoint dir", "dir", cm.checkpointDir, "error", err)
		return err
	}
	for _, f := range entries {
		if f.IsDir() {
			if ckptLayer, err = newCheckpointLayer(cm, cm.checkpointDir+"/"+f.Name()); err != nil {
				log.Warn("Failed to new checkpoint layer", "error", err)
				return err
			}
			cm.checkpointMap[ckptLayer.rootHash()] = ckptLayer
			succeedLoadNumber += 1
		}
	}

	return nil
}

// addCheckpoint adds a checkpoint. Note that this is a synchronous, blocking, and time-consuming operation.
func (cm *checkpointManager) addCheckpoint(blockNumber uint64, blockRoot common.Hash) error {
	if !cm.enableCheckPoint.Load() {
		return nil
	}

	var (
		err                         error
		startTimestamp              time.Time
		startNewCheckpointTimestamp time.Time
		endNewCheckpointTimestamp   time.Time
		subCheckpointDir            string
		ckptLayer                   *checkpointLayer
	)

	startTimestamp = time.Now()
	defer func() {
		addCheckpointTimer.UpdateSince(startTimestamp)
		newCheckpointTimer.Update(endNewCheckpointTimestamp.Sub(startNewCheckpointTimestamp))
		log.Info("Add a new checkpoint", "block_number", blockNumber, "block_root", blockRoot.String(),
			"error", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)),
			"new_elapsed", common.PrettyDuration(endNewCheckpointTimestamp.Sub(startNewCheckpointTimestamp)))
	}()

	subCheckpointDir = encodeSubCheckpointDir(cm.checkpointDir, blockNumber, blockRoot)
	startNewCheckpointTimestamp = time.Now()
	err = cm.db.NewCheckpoint(subCheckpointDir)
	endNewCheckpointTimestamp = time.Now()
	if err != nil {
		log.Warn("Failed to create checkpoint", "error", err)
		return err
	}

	ckptLayer, err = newCheckpointLayer(cm, subCheckpointDir)
	if err != nil {
		log.Warn("Failed to make checkpoint db", "error", err)
		return err
	}

	cm.mux.Lock()
	defer cm.mux.Unlock()
	cm.checkpointMap[blockRoot] = ckptLayer
	return nil
}

// getCheckpointLayer gets a checkpoint layer which is used to get checkpoint withdraw proof.
func (cm *checkpointManager) getCheckpointLayer(root common.Hash) (layer, error) {
	if !cm.enableCheckPoint.Load() {
		return nil, errors.New("checkpoint manager is disabled")
	}

	var (
		err            error
		startTimestamp time.Time
		ckptLayer      *checkpointLayer
	)

	startTimestamp = time.Now()
	defer func() {
		getCheckpointTimer.UpdateSince(startTimestamp)
		log.Info("Get the checkpoint", "root", root.String(),
			"elapsed", common.PrettyDuration(time.Since(startTimestamp)), "error", err)
	}()

	cm.mux.RLock()
	defer cm.mux.RUnlock()
	ckptLayer = cm.checkpointMap[root]
	if ckptLayer == nil {
		err = errors.New("checkpoint cannot be found")
	}
	return ckptLayer, err
}
