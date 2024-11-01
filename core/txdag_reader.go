package core

import (
	"bufio"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

var TxDAGCacheSize = uint64(300)

type TxDAGFileReader struct {
	output               string
	file                 *os.File
	scanner              *bufio.Scanner
	dagChan              chan *TxDAGOutputItem
	chanFirstBlockNumber int64
	latest               uint64
	lock                 sync.RWMutex
	isInit               bool
	closeChan            chan struct{}
}

func NewTxDAGFileReader(output string) (*TxDAGFileReader, error) {
	reader := &TxDAGFileReader{
		output:               output,
		dagChan:              make(chan *TxDAGOutputItem, TxDAGCacheSize),
		closeChan:            make(chan struct{}, 1),
		chanFirstBlockNumber: -1,
	}
	err := reader.openFile(output)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (t *TxDAGFileReader) Close() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.closeChan <- struct{}{}
	t.closeFile()
}

func (t *TxDAGFileReader) openFile(output string) error {
	file, err := os.Open(output)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 5*1024*1024), 5*1024*1024)
	t.file = file
	t.scanner = scanner
	return nil
}

func (t *TxDAGFileReader) closeFile() {
	if t.scanner != nil {
		t.scanner = nil
	}
	if t.file != nil {
		t.file.Close()
		t.file = nil
	}
}

func (t *TxDAGFileReader) Latest() uint64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.latest
}
func (t *TxDAGFileReader) InitAndStartReadingLock(startBlockNum uint64) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.initAndStartReading(startBlockNum)
}
func (t *TxDAGFileReader) initAndStartReading(startBlockNum uint64) error {
	if t.isInit {
		return nil
	}
	if t.scanner == nil {
		return errors.New("TxDAG reader init fail,missing scanner")
	}
	if startBlockNum > 0 {
		//We move the scanner to the position of startBlockNum-1 so that we can start reading data from startBlockNum next.
		startBlockNum = startBlockNum - 1
		for t.scanner != nil && t.scanner.Scan() {
			text := t.scanner.Text()
			blockNum, err := readTxDAGBlockNumFromLine(text)
			if err != nil {
				log.Error("TxDAG reader init fail at readTxDAGBlockNumFromLine", "err", err, "text", text)
				return err
			}
			if startBlockNum > blockNum {
				continue
			}
			t.latest = blockNum
			break
		}
		if t.scanner != nil && t.scanner.Err() != nil {
			log.Error("TxDAG reader init, scan TxDAG file got err", "err", t.scanner.Err(), "startBlockNum", startBlockNum, "latest", t.latest)
			return t.scanner.Err()
		}
	}
	log.Info("TxDAG reader init done", "startBlockNum", startBlockNum, "latest", t.latest)
	go t.loopReadDAGIntoChan()
	t.isInit = true
	return nil
}

func (t *TxDAGFileReader) loopReadDAGIntoChan() {
	start := time.Now()

	for t.scanner != nil && t.scanner.Scan() {
		select {
		case <-t.closeChan:
			close(t.dagChan)
			log.Info("TxDAG reader is closed. Exiting...", "latest", t.latest)
			return
		default:
			text := t.scanner.Text()
			num, dag, err := readTxDAGItemFromLine(text)
			if err != nil {
				log.Error("query TxDAG error", "latest", t.latest, "err", err)
				continue
			}
			t.dagChan <- &TxDAGOutputItem{blockNumber: num, txDAG: dag}
			t.latest = num
			if t.chanFirstBlockNumber == -1 {
				t.chanFirstBlockNumber = int64(num)
			}
			if time.Since(start) > 1*time.Minute {
				log.Debug("TxDAG reader dagChan report", "dagChanSize", len(t.dagChan), "latest", t.latest, "chanFirstBlockNumber", t.chanFirstBlockNumber)
				txDAGReaderChanGauge.Update(int64(len(t.dagChan)))
				start = time.Now()
			}
		}
	}
	if t.scanner != nil && t.scanner.Err() != nil {
		log.Error("scan TxDAG file got err", "latest", t.latest, "err", t.scanner.Err())
	} else {
		log.Info("TxDAG reader done. Exiting...", "latest", t.latest)
	}
}

func (t *TxDAGFileReader) TxDAG(expect uint64) types.TxDAG {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.isInit {
		log.Error("TxDAG reader not init yet")
		return nil
	}

	if t.scanner == nil {
		return nil
	}

	if t.chanFirstBlockNumber > int64(expect) {
		log.Debug("expect less than chanFirstBlockNumber,skip", "expect", expect, "chanFirstBlockNumber", t.chanFirstBlockNumber)
		return nil
	}

	for {
		select {
		case dag := <-t.dagChan:
			if dag == nil {
				return nil
			}
			t.chanFirstBlockNumber = int64(dag.blockNumber + 1)
			if dag.blockNumber < expect {
				continue
			} else if dag.blockNumber > expect {
				log.Warn("dag.blockNumber > expect", "dag.blockNumber", dag.blockNumber, "expect", expect, "chanFirstBlockNumber", t.chanFirstBlockNumber)
				return nil
			} else {
				return dag.txDAG
			}
		default:
			return nil
		}
	}
}

func (t *TxDAGFileReader) Reset(number uint64) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.closeChan <- struct{}{}
	t.closeFile()
	if err := t.openFile(t.output); err != nil {
		return err
	}
	t.latest = 0
	t.chanFirstBlockNumber = -1
	t.isInit = false
	t.closeChan = make(chan struct{}, 1)
	t.dagChan = make(chan *TxDAGOutputItem, TxDAGCacheSize)
	err := t.initAndStartReading(number)
	if err != nil {
		return err
	}
	return nil
}

func readTxDAGBlockNumFromLine(line string) (uint64, error) {
	tokens := strings.Split(line, ",")
	if len(tokens) != 2 {
		return 0, errors.New("txDAG output contain wrong size")
	}
	num, err := strconv.Atoi(tokens[0])
	if err != nil {
		return 0, err
	}
	return uint64(num), nil
}

func readTxDAGItemFromLine(line string) (uint64, types.TxDAG, error) {
	tokens := strings.Split(line, ",")
	if len(tokens) != 2 {
		return 0, nil, errors.New("txDAG output contain wrong size")
	}
	num, err := strconv.Atoi(tokens[0])
	if err != nil {
		return 0, nil, err
	}
	enc, err := hex.DecodeString(tokens[1])
	if err != nil {
		return 0, nil, err
	}
	txDAG, err := types.DecodeTxDAG(enc)
	if err != nil {
		return 0, nil, err
	}
	return uint64(num), txDAG, nil
}
