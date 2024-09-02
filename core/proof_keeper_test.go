package core

import (
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/stretchr/testify/assert"
)

const (
	testProofKeeperDBDir = "./test_proof_keeper_db"
)

var (
	mockBlockChain   *BlockChain
	mockKeeperMetaDB ethdb.Database
)

func setupTestEnv() {
	mockKeeperMetaDB, _ = rawdb.Open(rawdb.OpenOptions{
		Type:              "pebble",
		Directory:         testProofKeeperDBDir,
		AncientsDirectory: testProofKeeperDBDir + "/ancient",
		Namespace:         "test_proof_keeper",
		Cache:             10,
		Handles:           10,
		ReadOnly:          false,
	})
}

func cleanupTestEnv() {
	mockKeeperMetaDB.Close()
	os.RemoveAll(testProofKeeperDBDir)
}

func TestProofKeeperStartAndStop(t *testing.T) {
	setupTestEnv()

	keeperOpts := &proofKeeperOptions{
		enable:             true,
		keepProofBlockSpan: 100,
		watchStartKeepCh:   make(chan *pathdb.KeepRecord),
		notifyFinishKeepCh: make(chan error),
	}
	keeper := newProofKeeper(keeperOpts)
	assert.NotNil(t, keeper)

	err := keeper.Start(mockBlockChain, mockKeeperMetaDB)
	assert.Nil(t, err)

	err = keeper.Stop()
	assert.Nil(t, err)

	cleanupTestEnv()
}

func TestProofKeeperGC(t *testing.T) {
	setupTestEnv()
	keeperOpts := &proofKeeperOptions{
		enable:             true,
		keepProofBlockSpan: 100,
		gcInterval:         1,
		watchStartKeepCh:   make(chan *pathdb.KeepRecord),
		notifyFinishKeepCh: make(chan error),
	}
	keeper := newProofKeeper(keeperOpts)
	assert.NotNil(t, keeper)

	err := keeper.Start(mockBlockChain, mockKeeperMetaDB)
	assert.Nil(t, err)

	for i := uint64(1); i <= 100; i++ {
		keeper.putKeeperMetaRecord(&keeperMetaRecord{
			BlockID:      i,
			ProofID:      i - 1,
			KeepInterval: 1,
		})
		keeper.putProofDataRecord(&proofDataRecord{
			ProofID:      i - 1,
			BlockID:      i,
			StateRoot:    common.Hash{},
			Address:      common.Address{},
			AccountProof: nil,
			Balance:      nil,
			CodeHash:     common.Hash{},
			Nonce:        0,
			StorageHash:  common.Hash{},
			StorageProof: nil,
		})
	}
	keeper.latestBlockID = 100
	time.Sleep(2 * time.Second) // wait gc loop

	// no gc, becase keeper.latestBlockID <= keeper.opts.keepProofBlockSpan
	metaList := keeper.getKeeperMetaRecordList()
	assert.Equal(t, 100, len(metaList))

	for i := uint64(101); i <= 105; i++ {
		keeper.putKeeperMetaRecord(&keeperMetaRecord{
			BlockID:      i,
			ProofID:      i - 1,
			KeepInterval: 1,
		})
		keeper.putProofDataRecord(&proofDataRecord{
			ProofID:      i - 1,
			BlockID:      i,
			StateRoot:    common.Hash{},
			Address:      common.Address{},
			AccountProof: nil,
			Balance:      nil,
			CodeHash:     common.Hash{},
			Nonce:        0,
			StorageHash:  common.Hash{},
			StorageProof: nil,
		})
	}

	keeper.latestBlockID = 105
	time.Sleep(2 * time.Second) // wait gc loop

	// gc keep meta which block_id < 5(latestBlockID - keepProofBlockSpan), and 1/2/3/4 blockid keeper meta is truncated.
	metaList = keeper.getKeeperMetaRecordList()
	assert.Equal(t, 101, len(metaList))

	// gc proof data, truncate proof id = 4, and 0/1/2/3 proofid proof data is truncated.
	assert.NotNil(t, keeper.getProofDataRecord(4))
	assert.Nil(t, keeper.getProofDataRecord(3))

	err = keeper.Stop()
	assert.Nil(t, err)

	cleanupTestEnv()
}

func TestProofKeeperQuery(t *testing.T) {
	setupTestEnv()

	keeperOpts := &proofKeeperOptions{
		enable:             true,
		watchStartKeepCh:   make(chan *pathdb.KeepRecord),
		notifyFinishKeepCh: make(chan error),
	}
	keeper := newProofKeeper(keeperOpts)
	assert.NotNil(t, keeper)

	err := keeper.Start(mockBlockChain, mockKeeperMetaDB)
	assert.Nil(t, err)

	for i := uint64(1); i <= 100; i++ {
		if i%15 == 0 {
			keeper.putKeeperMetaRecord(&keeperMetaRecord{
				BlockID:      i,
				ProofID:      i - 1,
				KeepInterval: 1,
			})
		}
		keeper.putProofDataRecord(&proofDataRecord{
			ProofID:      i - 1,
			BlockID:      i,
			StateRoot:    common.Hash{},
			Address:      common.Address{},
			AccountProof: nil,
			Balance:      nil,
			CodeHash:     common.Hash{},
			Nonce:        0,
			StorageHash:  common.Hash{},
			StorageProof: nil,
		})

	}

	keeper.latestBlockID = 100
	result, err := keeper.QueryProposeProof(45, common.Hash{})
	assert.Nil(t, err)
	assert.NotNil(t, result)
	result, err = keeper.QueryProposeProof(46, common.Hash{})
	assert.Nil(t, err)
	assert.NotNil(t, result)
	result, err = keeper.QueryProposeProof(1, common.Hash{}) //  should >= 15
	assert.NotNil(t, err)
	assert.Nil(t, result)
	result, err = keeper.QueryProposeProof(100, common.Hash{})
	assert.Nil(t, err)
	assert.NotNil(t, result)
	result, err = keeper.QueryProposeProof(101, common.Hash{}) //  should <= 100
	assert.NotNil(t, err)
	assert.Nil(t, result)

	err = keeper.Stop()
	assert.Nil(t, err)

	cleanupTestEnv()
}
