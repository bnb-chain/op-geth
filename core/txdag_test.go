package core

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

func TestAnalyzeDAGFile(t *testing.T) {
	fromBlock := uint64(8900000)
	toBlock := uint64(9200000)
	inputFilePath := "/Users/welkin/Downloads/parallel-txdag-output_compare_15000000.csv"
	outputFilePath := "./dagAnalyzeResult-890w-920w.csv"
	analyzeDAGFile(fromBlock, toBlock, inputFilePath, outputFilePath, t)
}

func analyzeDAGFile(fromBlock uint64, toBlock uint64, inputFilePath string, outputFilePath string, t *testing.T) {
	scanner, file, err := openFile(inputFilePath)
	if err != nil {
		t.Error("Failed to open dag file", "err", err)
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			t.Error("Failed to close dag file", "err", err)
			return
		}
	}()
	var walkBlock uint64
	if fromBlock == 0 {
		walkBlock = 0
	} else {
		walkBlock = fromBlock - 1
	}
	err = walkScannerToBlock(scanner, walkBlock)
	if err != nil {
		t.Error("Failed to walk dag file to block", "err", err, "targetBlock", walkBlock-1)
		return
	}
	writer, outputFile, err := newCSVWriter(outputFilePath)
	if err != nil {
		t.Error("Failed to create output file", "err", err, "outputFilePath", outputFilePath)
		return
	}
	defer func() {
		err := outputFile.Close()
		if err != nil {
			t.Error("Failed to close output file", "err", err)
			return
		}
	}()
	err = scanAndOutput(scanner, writer, toBlock)
	if err != nil {
		t.Error("Failed to scan dag file to csv", "err", err, "toBlock", toBlock)
		return
	}
}

type oneLine struct {
	block         uint64
	depList       [][]uint64
	flags         []uint8
	emptyDepRate  float64
	totalTxCount  int
	txLevelLength int
	txLevelDetail levelDetails
}

func (l *oneLine) toCsvStringList() []string {
	var result []string
	result = append(result, fmt.Sprintf("%d", l.block))
	result = append(result, fmt.Sprintf("%v", l.depList))
	result = append(result, fmt.Sprintf("%v", l.flags))
	result = append(result, fmt.Sprintf("%.4f", l.emptyDepRate))
	result = append(result, fmt.Sprintf("%v", l.totalTxCount))
	result = append(result, fmt.Sprintf("%v", l.txLevelLength))
	result = append(result, fmt.Sprintf("%v", l.txLevelDetail))
	return result
}

type levelDetails []*levelDetail

func (l levelDetails) String() string {
	sb := strings.Builder{}
	for i, detail := range l {
		sb.WriteString(detail.String())
		if i < len(l)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

type levelDetail struct {
	idx  int
	size int
}

func (l *levelDetail) String() string {
	return fmt.Sprintf("%d:%d", l.idx, l.size)
}

func scanAndOutput(scanner *bufio.Scanner, writer *csv.Writer, toBlock uint64) error {
	defer writer.Flush()
	err := writer.Write([]string{"block", "depList", "flags", "emptyDepRate", "totalTxCount", "txLevelLength", "txLevelDetail"})
	if err != nil {
		return err
	}
	for scanner.Scan() {
		line := scanner.Text()
		number, dag, err := readTxDAGItemFromLine(line)
		if err != nil {
			return err
		}
		if number > toBlock {
			return nil
		}
		csvOneLine := dagToCSVOneLine(number, dag)
		csvStringList := csvOneLine.toCsvStringList()
		err = writer.Write(csvStringList)
		if err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func dagToCSVOneLine(number uint64, dag types.TxDAG) *oneLine {
	depList := make([][]uint64, 0, dag.TxCount())
	flags := make([]uint8, 0, dag.TxCount())
	emptyDepCount := 0
	var reqs []*PEVMTxRequest
	for i := 0; i < dag.TxCount(); i++ {
		txIdxs := types.TxDependency(dag, i)
		dep := dag.TxDep(i)
		depList = append(depList, txIdxs)
		if dep.Count() == 0 {
			emptyDepCount++
		}
		if dep.Flags != nil {
			flags = append(flags, *dep.Flags)
		} else {
			flags = append(flags, 0)
		}
		reqs = append(reqs, &PEVMTxRequest{txIndex: i})
	}

	txLevels := NewTxLevels(reqs, dag)
	var txLevelDetail []*levelDetail
	for idx, oneLevel := range txLevels {
		txLevelDetail = append(txLevelDetail, &levelDetail{idx: idx, size: len(oneLevel)})
	}
	return &oneLine{
		block:         number,
		depList:       depList,
		flags:         flags,
		emptyDepRate:  float64(emptyDepCount) / float64(dag.TxCount()),
		totalTxCount:  dag.TxCount(),
		txLevelLength: len(txLevels),
		txLevelDetail: txLevelDetail,
	}
}

func newCSVWriter(filePath string) (*csv.Writer, *os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, nil, err
	}
	writer := csv.NewWriter(file)
	return writer, file, nil
}

func walkScannerToBlock(scanner *bufio.Scanner, targetBlock uint64) error {
	for scanner.Scan() {
		line := scanner.Text()
		blockNum, err := readTxDAGBlockFromLine(line)
		if err != nil {
			return err
		}
		if blockNum >= targetBlock {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func openFile(filePath string) (*bufio.Scanner, *os.File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 5*1024*1024), 5*1024*1024)
	return scanner, file, nil
}
