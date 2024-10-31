package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTxDAGFileReader(t *testing.T) {
	reader, err := NewTxDAGFileReader("./txdag_test_case.csv")
	if err != nil {
		t.Error("newReaderErr", "err", err)
	}
	err = reader.InitAndStartReadingLock(10)
	if err != nil {
		t.Error("newReaderErr", "err", err)
	}
	//Check the initialization status
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	//Waiting for the first data to enter the channel
	for reader.chanFirstBlockNumber == -1 {
		time.Sleep(10 * time.Millisecond)
	}
	//The starting point is 10, so 9 should not exist, 10 should exist, and txCount==1
	dag9 := reader.TxDAG(9)
	assert.Nil(t, dag9)
	dag10 := reader.TxDAG(10)
	assert.NotNil(t, dag10)
	assert.Equal(t, 1, dag10.TxCount())
	//Waiting to process to 20
	for reader.latest < 20 {
		time.Sleep(10 * time.Millisecond)
	}
	//There are 9 transactions in 20
	dag20 := reader.TxDAG(20)
	assert.NotNil(t, dag20)
	assert.Equal(t, 9, dag20.TxCount())
	//Already read 20, data less than 20 cannot be read.
	dag11 := reader.TxDAG(11)
	assert.Nil(t, dag11)
	err = reader.Reset(10)
	if err != nil {
		t.Error("resetErr", "err", err)
	}
	//Check the initialization status again after reset
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	//Waiting for 11 to be read
	for reader.latest < 11 {
		time.Sleep(10 * time.Millisecond)
	}
	//11 should no longer be nil, because after reset, we haven't read it yet.
	dag11 = reader.TxDAG(11)
	assert.NotNil(t, dag11)
	assert.Equal(t, 1, dag11.TxCount())
	//1000 should not exist
	dag1000 := reader.TxDAG(1000)
	assert.Nil(t, dag1000)
}

func TestTxDAGFileReader_Close(t *testing.T) {
	reader, err := NewTxDAGFileReader("./txdag_test_case.csv")
	if err != nil {
		t.Error("newReaderErr", "err", err)
	}
	err = reader.InitAndStartReadingLock(10)
	if err != nil {
		t.Error("newReaderErr", "err", err)
	}
	assert.Equal(t, true, reader.isInit)
	assert.NotNil(t, reader.scanner)
	reader.Close()
	assert.Nil(t, reader.scanner)
	assert.Nil(t, reader.file)
}
