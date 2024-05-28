package rawdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testAncientProofDir = "./test_ancient_proof"
)

var (
	testAncientProofDB *ResettableFreezer
	mockData1          = []byte{'a'}
	mockData2          = []byte{'1'}
)

func setupTestEnv() {
	testAncientProofDB, _ = NewProofFreezer(testAncientProofDir, false)
}

func cleanupTestEnv() {
	testAncientProofDB.Close()
	os.RemoveAll(testAncientProofDir)
}

func TestProofDataAPI(t *testing.T) {
	setupTestEnv()
	var proofData []byte

	// case1: empty db
	proofData = GetLatestProofData(testAncientProofDB)
	assert.Nil(t, proofData)

	// case2: mismatch sequence put failed
	mismatchProofID := uint64(2) // should=0
	err := PutProofData(testAncientProofDB, mismatchProofID, mockData1)
	assert.NotNil(t, err)
	proofData = GetLatestProofData(testAncientProofDB)
	assert.Nil(t, proofData)

	// case3: put/get succeed
	matchProofID := uint64(0)
	err = PutProofData(testAncientProofDB, matchProofID, mockData1)
	assert.Nil(t, err)
	err = PutProofData(testAncientProofDB, matchProofID+1, mockData2)
	assert.Nil(t, err)
	proofData = GetLatestProofData(testAncientProofDB)
	assert.Equal(t, proofData, mockData2)
	proofData = GetProofData(testAncientProofDB, 0)
	assert.Equal(t, proofData, mockData1)
	proofData = GetProofData(testAncientProofDB, 1)
	assert.Equal(t, proofData, mockData2)

	// case4: truncate head
	TruncateProofDataHead(testAncientProofDB, 1)
	proofData = GetProofData(testAncientProofDB, 1)
	assert.Nil(t, proofData)
	proofData = GetProofData(testAncientProofDB, 0)
	assert.Equal(t, proofData, mockData1)

	// case5: restart
	testAncientProofDB.Close()
	setupTestEnv()
	proofData = GetProofData(testAncientProofDB, 0)
	assert.Equal(t, proofData, mockData1)
	proofData = GetLatestProofData(testAncientProofDB)
	assert.Equal(t, proofData, mockData1)

	// case6: truncate tail
	PutProofData(testAncientProofDB, matchProofID+1, mockData2)
	proofData = GetProofData(testAncientProofDB, matchProofID)
	assert.Equal(t, proofData, mockData1)
	PutProofData(testAncientProofDB, matchProofID+2, mockData2)
	TruncateProofDataTail(testAncientProofDB, matchProofID+1)
	proofData = GetProofData(testAncientProofDB, matchProofID)
	assert.Nil(t, proofData)
	proofData = GetProofData(testAncientProofDB, matchProofID+1)
	assert.Equal(t, proofData, mockData2)
	TruncateProofDataTail(testAncientProofDB, matchProofID+2)
	proofData = GetProofData(testAncientProofDB, matchProofID+1)
	assert.Nil(t, proofData)

	cleanupTestEnv()
}
