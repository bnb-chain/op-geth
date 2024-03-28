package pathdb

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/assert"
)

const (
	originTestDBDir     = "./origin_test_db"
	checkpointTestDBDir = "./checkpoint_test_db"
)

var (
	testDB ethdb.Database
)

func setupTestEnv() {
	opt := rawdb.OpenOptions{
		Type:      "pebble",
		Directory: originTestDBDir,
		ReadOnly:  false,
	}
	tmpDB, err := rawdb.Open(opt)
	if err != nil {
		log.Warn("Failed to open rawdb", "err", err)
		return
	}
	testDB = tmpDB
	os.Mkdir(checkpointTestDBDir, 0777)
}

func cleanupTestEnv() {
	testDB.Close()
	os.RemoveAll(originTestDBDir)
	os.RemoveAll(checkpointTestDBDir)
}

func TestNewCheckpointManager(t *testing.T) {
	setupTestEnv()
	assert.NotNil(t, testDB)
	ckptManager := newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 10)
	assert.NotNil(t, ckptManager)
	cleanupTestEnv()
}

func TestAddCheckpoint(t *testing.T) {
	setupTestEnv()
	// case1, checkpoint_manager is disabled
	assert.NotNil(t, testDB)
	ckptManager := newCheckpointManager(testDB, checkpointTestDBDir, false, 10, 10)
	assert.NotNil(t, ckptManager)
	err := ckptManager.addCheckpoint(10, common.BytesToHash([]byte("123")))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ckptManager.checkpointMap))

	// case2, checkpoint_manager is enabled
	ckptManager = newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 10)
	assert.NotNil(t, ckptManager)
	err = ckptManager.addCheckpoint(10, common.BytesToHash([]byte("123")))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ckptManager.checkpointMap))

	cleanupTestEnv()
}

func TestGCCheckpoint(t *testing.T) {
	setupTestEnv()
	assert.NotNil(t, testDB)
	ckptManager := newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 3)
	assert.NotNil(t, ckptManager)

	addCheckpointCounter := 0
	for i := uint64(1); i <= 100; i++ {
		if ckptManager.needDoCheckpoint(i) {
			err := ckptManager.addCheckpoint(i, common.BytesToHash([]byte(strconv.FormatUint(i, 10))))
			assert.Nil(t, err)
			addCheckpointCounter++
		}
	}
	assert.Equal(t, 10, addCheckpointCounter)

	time.Sleep(10 * time.Second) // wait for gc loop

	assert.Equal(t, 3, len(ckptManager.checkpointMap))
	cleanupTestEnv()
}

func TestLoadCheckpoint(t *testing.T) {
	setupTestEnv()
	assert.NotNil(t, testDB)
	ckptManager := newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 10)
	assert.NotNil(t, ckptManager)

	addCheckpointCounter := 0
	for i := uint64(1); i <= 100; i++ {
		if ckptManager.needDoCheckpoint(i) {
			err := ckptManager.addCheckpoint(i, common.BytesToHash([]byte(strconv.FormatUint(i, 10))))
			assert.Nil(t, err)
			addCheckpointCounter++
		}
	}
	assert.Equal(t, 10, addCheckpointCounter)
	ckptManager.close()
	assert.Equal(t, 0, len(ckptManager.checkpointMap))

	// load checkpoint case
	ckptManager2 := newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 10)
	assert.NotNil(t, ckptManager2)
	assert.Equal(t, 10, len(ckptManager2.checkpointMap))
	cleanupTestEnv()
}

func TestCheckpointReader(t *testing.T) {
	var (
		testPathKey       = []byte("123")
		testNodeValue     = []byte("abc")
		testNodeValueHash common.Hash
		testBlockRoot     = common.BytesToHash([]byte(strconv.FormatUint(10, 10)))
	)

	setupTestEnv()
	assert.NotNil(t, testDB)
	rawdb.WriteAccountTrieNode(testDB, testPathKey, testNodeValue)
	value, testNodeValueHash := rawdb.ReadAccountTrieNode(testDB, testPathKey)
	assert.Equal(t, testNodeValue, value)

	ckptManager := newCheckpointManager(testDB, checkpointTestDBDir, true, 10, 10)
	assert.NotNil(t, ckptManager)
	err := ckptManager.addCheckpoint(10, testBlockRoot)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ckptManager.checkpointMap))

	// has deleted
	rawdb.DeleteAccountTrieNode(testDB, testPathKey)
	value, _ = rawdb.ReadAccountTrieNode(testDB, testPathKey)
	assert.Nil(t, value)

	// query checkpoint case
	ckptLayer, err := ckptManager.getCheckpointLayer(testBlockRoot)
	assert.Nil(t, err)

	value, err = ckptLayer.Node(common.Hash{}, testPathKey, testNodeValueHash)
	assert.Nil(t, err)
	assert.Equal(t, testNodeValue, value)
	cleanupTestEnv()
}
