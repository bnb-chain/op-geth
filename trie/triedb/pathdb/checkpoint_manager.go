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
	// DefaultMaxCheckpointNumber is used to keep checkpoint number, default is 7 days for main-net.
	DefaultMaxCheckpointNumber = 24 * 7
	// DefaultCheckpointDir is used to store checkpoint directory.
	DefaultCheckpointDir = "checkpoint"

	gcCheckpointIntervalSecond        = 1
	printCheckpointStatIntervalSecond = 60
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

func makeCheckpointDB(checkpointDir string) (ckptLayer *checkpointLayer, err error) {
	var (
		blockNumber uint64
		blockRoot   common.Hash
		ckptDB      ethdb.Database
	)

	if blockNumber, blockRoot, err = decodeSubCheckpointDir(checkpointDir); err != nil {
		log.Warn("Failed to decode checkpoint dir", "dir", checkpointDir, "err", err)
		return nil, err
	}
	if ckptDB, err = rawdb.Open(rawdb.OpenOptions{
		Type:      "pebble",
		Directory: checkpointDir,
		ReadOnly:  true,
	}); err != nil {
		log.Warn("Failed to open raw db", "err", err)
		return nil, err
	}
	ckptLayer = &checkpointLayer{
		blockNumber:  blockNumber,
		root:         blockRoot,
		checkpointDB: ckptDB,
	}
	return ckptLayer, nil
}

// only provide node interface
type checkpointLayer struct {
	blockNumber  uint64
	root         common.Hash
	checkpointDB ethdb.Database
}

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
		ckptLayer.checkpointDB.Close()
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

func (cm *checkpointManager) gcCheckpoint() error {
	if !cm.enableCheckPoint.Load() {
		return nil
	}
	if len(cm.checkpointMap) <= int(cm.maxCheckpointNumber) {
		return nil
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
			log.Info("GC a checkpoint", "checkpoint_block_number", gcCkptLayer.blockNumber,
				"gc_checkpoint_dir", gcCheckpointDir,
				"elapsed", common.PrettyDuration(time.Since(startTimestamp)),
				"err", err)
		}()

		cm.mux.Lock()
		delete(cm.checkpointMap, gcCkptLayer.rootHash())
		cm.mux.Unlock()

		if err = gcCkptLayer.checkpointDB.Close(); err != nil {
			log.Warn("Failed to close checkpoint db", "err", err)
			return err
		}
		gcCheckpointDir = encodeSubCheckpointDir(cm.checkpointDir, gcCkptLayer.blockNumber, gcCkptLayer.root)
		err = os.RemoveAll(gcCheckpointDir)
	}
	return nil
}

func (cm *checkpointManager) printCheckpointStat() error {
	if !cm.enableCheckPoint.Load() {
		return nil
	}

	cm.logDetailedInfo()
	cm.mux.RLock()
	checkpointSizeGauge.Update(int64(len(cm.checkpointMap)))
	for _, ckptLayer := range cm.checkpointMap {
		log.Info("Checkpoint detailed info", "checkpoint_block_number", ckptLayer.blockNumber, "checkpoint_block_root", ckptLayer.root.String())
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
			"elapsed", common.PrettyDuration(time.Since(startTimestamp)), "err", err)
	}()

	dir, err := ioutil.ReadDir(cm.checkpointDir)
	if err != nil {
		log.Warn("Failed to scan checkpoint dir", "dir", cm.checkpointDir, "err", err)
		return err
	}
	for _, f := range dir {
		if f.IsDir() {
			if ckptLayer, err = makeCheckpointDB(cm.checkpointDir + "/" + f.Name()); err != nil {
				log.Warn("Failed to make checkpoint", "err", err)
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
		err              error
		startTimestamp   time.Time
		subCheckpointDir string
		ckptLayer        *checkpointLayer
	)

	startTimestamp = time.Now()
	defer func() {
		addCheckpointTimer.UpdateSince(startTimestamp)
		log.Info("Add checkpoint", "block_number", blockNumber, "block_root", blockRoot.String(),
			"elapsed", common.PrettyDuration(time.Since(startTimestamp)), "err", err)
	}()

	subCheckpointDir = encodeSubCheckpointDir(cm.checkpointDir, blockNumber, blockRoot)
	if err = cm.db.NewCheckpoint(subCheckpointDir); err != nil {
		log.Warn("Failed to create checkpoint", "err", err)
		return err
	}

	if ckptLayer, err = makeCheckpointDB(subCheckpointDir); err != nil {
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
		log.Info("Get checkpoint", "root", root.String(),
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
