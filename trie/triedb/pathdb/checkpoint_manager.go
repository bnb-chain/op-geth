package pathdb

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
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
	// checkpointDBKeepaliveSecond is used to check db which has been not used for a long time,
	// and close it for reducing memory.
	checkpointDBKeepaliveSecond = 60
)

func encodeSubCheckpointDir(checkpointDir string, blockNumber uint64, blockRoot common.Hash) string {
	return checkpointDir + "/" + strconv.FormatUint(blockNumber, 10) + "_" + blockRoot.String()
}

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
}

var _ layer = &checkpointLayer{}

func newCheckpointLayer(checkpointDir string) (ckptLayer *checkpointLayer, err error) {
	var (
		blockNumber uint64
		blockRoot   common.Hash
	)

	if blockNumber, blockRoot, err = decodeSubCheckpointDir(checkpointDir); err != nil {
		log.Warn("Failed to decode checkpoint dir", "dir", checkpointDir, "err", err)
		return nil, err
	}
	ckptLayer = &checkpointLayer{
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
	go ckptLayer.loop()
	return ckptLayer, nil
}

func (c *checkpointLayer) loop() {
	var (
		err                            error
		startOpenCheckpointTimestamp   time.Time
		endOpenCheckpointTimestamp     time.Time
		startCloseCheckpointTimestamp  time.Time
		endCloseCheckpointTimestamp    time.Time
		startDeleteCheckpointTimestamp time.Time
		endDeleteCheckpointTimestamp   time.Time
	)

	tryCloseTicker := time.NewTicker(time.Second * checkpointDBKeepaliveSecond)

	for {
		select {
		case <-tryCloseTicker.C: // close db when a long time unused for reducing memory usage.
			if c.isOpened.Load() && time.Now().Sub(c.lastUsedTimestamp) > time.Second*checkpointDBKeepaliveSecond {
				if err = c.checkpointDB.Close(); err != nil {
					log.Warn("Failed to close checkpoint db", "err", err)
				} else {
					c.isOpened.Store(false)
				}
			}
		case <-c.tryOpenCh: // lazy to open db for reducing memory usage.
			c.lastUsedTimestamp = time.Now()
			if !c.isOpened.Load() {
				startOpenCheckpointTimestamp = time.Now()
				c.checkpointDB, err = rawdb.Open(rawdb.OpenOptions{
					Type:      "pebble",
					Directory: c.checkpointDir,
					ReadOnly:  true,
				})
				endOpenCheckpointTimestamp = time.Now()
				openCheckpointTimer.Update(endOpenCheckpointTimestamp.Sub(startOpenCheckpointTimestamp))
				if err != nil {
					log.Warn("Failed to open raw db", "err", err)
				} else {
					c.isOpened.Store(true)
				}

			}
			c.waitOpenCh <- struct{}{}
		case <-c.tryCloseAndDeleteCh: // gc for reducing disk usage.
			if c.isOpened.Load() {
				startCloseCheckpointTimestamp = time.Now()
				err = c.checkpointDB.Close()
				endCloseCheckpointTimestamp = time.Now()
				if err != nil {
					log.Warn("Failed to close checkpoint db", "err", err)
				} else {
					c.isOpened.Store(false)
				}
				log.Info("Close checkpoint db", "err", err,
					"elapsed", common.PrettyDuration(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp)))
				closeCheckpointTimer.Update(endCloseCheckpointTimestamp.Sub(startCloseCheckpointTimestamp))
			}
			if !c.isOpened.Load() {
				startDeleteCheckpointTimestamp = time.Now()
				err = os.RemoveAll(c.checkpointDir)
				endDeleteCheckpointTimestamp = time.Now()
				log.Info("Delete checkpoint db", "err", err,
					"elapsed", common.PrettyDuration(endDeleteCheckpointTimestamp.Sub(startDeleteCheckpointTimestamp)))
				deleteCheckpointTimer.Update(endDeleteCheckpointTimestamp.Sub(startDeleteCheckpointTimestamp))
			}
			c.waitCloseAndDeleteCh <- struct{}{}
		case <-c.tryCloseAndStopCh: // close for stop
			if c.isOpened.Load() {
				c.checkpointDB.Close()
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

func (c *checkpointLayer) rootHash() common.Hash {
	return c.root
}

// unused
func (c *checkpointLayer) stateID() uint64 {
	return 0
}

// unused
func (c *checkpointLayer) parentLayer() layer {
	return nil
}

// unused
func (c *checkpointLayer) update(common.Hash, uint64, uint64, map[common.Hash]map[string]*trienode.Node, *triestate.Set) *diffLayer {
	return nil
}

// unused
func (c *checkpointLayer) journal(io.Writer) error {
	return nil
}

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
}

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
	go cm.loop()
	cm.logDetailedInfo()
	log.Info("Succeed to new checkpoint manager", "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	return cm
}

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

func (cm *checkpointManager) needDoCheckpoint(currentCommitBlockNumber uint64) bool {
	if !cm.enableCheckPoint.Load() || cm.checkpointBlockInterval == 0 {
		return false
	}
	if currentCommitBlockNumber%cm.checkpointBlockInterval == 0 {
		return true
	}
	return false
}

func (cm *checkpointManager) loop() {
	gcCheckpointTicker := time.NewTicker(time.Second * gcCheckpointIntervalSecond)
	printCheckpointStatTicker := time.NewTicker(time.Second * printCheckpointStatIntervalSecond)

	for {
		select {
		case <-cm.stopCh:
			close(cm.waitCloseCh)
			return
		case <-gcCheckpointTicker.C:
			cm.gcCheckpoint()
		case <-printCheckpointStatTicker.C:
			cm.printCheckpointStat()
		}
	}
}

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
			err             error
			startTimestamp  time.Time
			gcCheckpointDir string
		)
		startTimestamp = time.Now()
		defer func() {
			gcCheckpointTimer.UpdateSince(startTimestamp)
			log.Info("GC one checkpoint", "checkpoint_block_number", gcCkptLayer.blockNumber,
				"gc_checkpoint_dir", gcCheckpointDir, "err", err,
				"elapsed", common.PrettyDuration(time.Since(startTimestamp)))
		}()

		cm.mux.Lock()
		delete(cm.checkpointMap, gcCkptLayer.rootHash())
		cm.mux.Unlock()
		gcCkptLayer.waitDBCloseAndDelete()
	}
}

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
			"err", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	}()

	dir, err := ioutil.ReadDir(cm.checkpointDir)
	if err != nil {
		log.Warn("Failed to scan checkpoint dir", "dir", cm.checkpointDir, "err", err)
		return err
	}
	for _, f := range dir {
		if f.IsDir() {
			if ckptLayer, err = newCheckpointLayer(cm.checkpointDir + "/" + f.Name()); err != nil {
				log.Warn("Failed to new checkpoint layer", "err", err)
				return err
			}
			cm.checkpointMap[ckptLayer.rootHash()] = ckptLayer
			succeedLoadNumber += 1
		}
	}

	return nil
}

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
			"err", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)),
			"new_elapsed", common.PrettyDuration(endNewCheckpointTimestamp.Sub(startNewCheckpointTimestamp)))
	}()

	subCheckpointDir = encodeSubCheckpointDir(cm.checkpointDir, blockNumber, blockRoot)
	startNewCheckpointTimestamp = time.Now()
	err = cm.db.NewCheckpoint(subCheckpointDir)
	endNewCheckpointTimestamp = time.Now()
	if err != nil {
		log.Warn("Failed to create checkpoint", "err", err)
		return err
	}

	ckptLayer, err = newCheckpointLayer(subCheckpointDir)
	if err != nil {
		log.Warn("Failed to make checkpoint db", "err", err)
		return err
	}

	cm.mux.Lock()
	defer cm.mux.Unlock()
	cm.checkpointMap[blockRoot] = ckptLayer
	return nil
}

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
			"elapsed", common.PrettyDuration(time.Since(startTimestamp)), "err", err)
	}()

	cm.mux.RLock()
	defer cm.mux.RUnlock()
	ckptLayer = cm.checkpointMap[root]
	if ckptLayer == nil {
		err = errors.New("checkpoint cannot be found")
	}
	return ckptLayer, err
}
