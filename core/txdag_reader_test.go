package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTxDAGFileReader(t *testing.T) {
	path := filepath.Join(os.TempDir(), "test.csv")
	defer func() {
		os.Remove(path)
	}()
	except := map[uint64]types.TxDAG{
		0: types.NewEmptyTxDAG(),
		1: makeEmptyPlainTxDAG(1),
		2: makeEmptyPlainTxDAG(2, types.NonDependentRelFlag),
		3: types.NewEmptyTxDAG(),
		4: makeEmptyPlainTxDAG(4, types.NonDependentRelFlag, types.ExcludedTxFlag),
		5: makeEmptyPlainTxDAG(5, types.NonDependentRelFlag, types.ExcludedTxFlag),
	}
	writeFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	require.NoError(t, err)
	for i := uint64(0); i < 6; i++ {
		require.NoError(t, writeTxDAGToFile(writeFile, TxDAGOutputItem{blockNumber: i, txDAG: except[i]}))
	}
	writeFile.Sync()
	writeFile.Close()

	reader, err := NewTxDAGFileReader(path)
	if err != nil {
		t.Error("newReaderErr", "err", err)
		return
	}
	err = reader.InitAndStartReadingLock(2)
	if err != nil {
		t.Error("newReaderErr", "err", err)
		return
	}
	//Check the initialization status
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	//Waiting for the first data to enter the channel
	for reader.chanFirstBlockNumber == -1 {
		time.Sleep(10 * time.Millisecond)
	}
	//The starting point is 2, so 1 should not exist, 2 should exist, and txCount==2
	dag1 := reader.TxDAG(1)
	assert.Nil(t, dag1)
	dag2 := reader.TxDAG(2)
	assert.NotNil(t, dag2)
	assert.Equal(t, 2, dag2.TxCount())
	//Waiting to process to 5
	for reader.latest < 5 {
		time.Sleep(10 * time.Millisecond)
	}
	//There are 9 transactions in 20
	dag5 := reader.TxDAG(5)
	assert.NotNil(t, dag5)
	assert.Equal(t, 5, dag5.TxCount())
	//Already read 5, data less than 5 cannot be read.
	dag3 := reader.TxDAG(3)
	assert.Nil(t, dag3)
	err = reader.Reset(2)
	if err != nil {
		t.Error("resetErr", "err", err)
		return
	}
	//Check the initialization status again after reset
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	//Waiting for 3 to be read
	for reader.latest < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	//11 should no longer be nil, because after reset, we haven't read it yet.
	dag3 = reader.TxDAG(3)
	assert.NotNil(t, dag3)
	assert.Equal(t, 0, dag3.TxCount())
	//1000 should not exist
	dag1000 := reader.TxDAG(1000)
	assert.Nil(t, dag1000)
}

func TestTxDAGFileReader_Close(t *testing.T) {
	path := filepath.Join(os.TempDir(), "test.csv")
	defer func() {
		os.Remove(path)
	}()
	except := map[uint64]types.TxDAG{
		0: types.NewEmptyTxDAG(),
		1: makeEmptyPlainTxDAG(1),
		2: makeEmptyPlainTxDAG(2, types.NonDependentRelFlag),
		3: types.NewEmptyTxDAG(),
		4: makeEmptyPlainTxDAG(4, types.NonDependentRelFlag, types.ExcludedTxFlag),
		5: makeEmptyPlainTxDAG(5, types.NonDependentRelFlag, types.ExcludedTxFlag),
	}
	writeFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	require.NoError(t, err)
	for i := uint64(0); i < 6; i++ {
		require.NoError(t, writeTxDAGToFile(writeFile, TxDAGOutputItem{blockNumber: i, txDAG: except[i]}))
	}
	writeFile.Sync()
	writeFile.Close()

	reader, err := NewTxDAGFileReader(path)
	if err != nil {
		t.Error("newReaderErr", "err", err)
		return
	}
	err = reader.InitAndStartReadingLock(2)
	if err != nil {
		t.Error("newReaderErr", "err", err)
		return
	}
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	reader.Close()
	assert.Nil(t, reader.scanner)
	assert.Nil(t, reader.file)
}
