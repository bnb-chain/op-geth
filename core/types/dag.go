package types

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/exp/slices"
)

const TxDAGAbiJson = `
[
  {
    "type": "function",
    "name": "setTxDAG",
    "inputs": [
      {
        "name": "data",
        "type": "bytes",
        "internalType": "bytes"
      }
    ],
    "outputs": [],
    "stateMutability": "nonpayable"
  }
]
`

var TxDAGABI abi.ABI

func init() {
	var err error
	// must be able to register the TxDAGABI
	TxDAGABI, err = abi.JSON(strings.NewReader(TxDAGAbiJson))
	if err != nil {
		panic(err)
	}
}

// TxDAGType Used to extend TxDAG and customize a new DAG structure
const (
	EmptyTxDAGType byte = iota
	PlainTxDAGType
)

var (
	// NonDependentRelFlag indicates that the txs described is non-dependent
	// and is used to reduce storage when there are a large number of dependencies.
	NonDependentRelFlag uint8 = 0x01
	// ExcludedTxFlag indicates that the tx is excluded from TxDAG, user should execute them in sequence.
	// These excluded transactions should be consecutive in the head or tail.
	ExcludedTxFlag uint8 = 0x02
	TxDepFlagMask        = NonDependentRelFlag | ExcludedTxFlag
)

type TxDAG interface {
	// Type return TxDAG type
	Type() byte

	// Inner return inner instance
	Inner() interface{}

	// DelayGasFeeDistribution check if delay the distribution of GasFee
	DelayGasFeeDistribution() bool

	// TxDep query TxDeps from TxDAG
	TxDep(int) *TxDep

	// TxCount return tx count
	TxCount() int

	// SetTxDep at the last one
	SetTxDep(int, TxDep) error
}

func DecodeTxDAGCalldata(data []byte) (TxDAG, error) {
	// trim the method id before unpack
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid txDAG calldata, len(data)=%d", len(data))
	}
	calldata, err := TxDAGABI.Methods["setTxDAG"].Inputs.Unpack(data[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to call abi unpack, err: %v", err)
	}
	if len(calldata) <= 0 {
		return nil, fmt.Errorf("invalid txDAG calldata, len(calldata)=%d", len(calldata))
	}
	data, ok := calldata[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid txDAG calldata parameter")
	}
	return DecodeTxDAG(data)
}

func EncodeTxDAGCalldata(dag TxDAG) ([]byte, error) {
	data, err := EncodeTxDAG(dag)
	if err != nil {
		return nil, fmt.Errorf("failed to encode txDAG, err: %v", err)
	}
	data, err = TxDAGABI.Pack("setTxDAG", data)
	if err != nil {
		return nil, fmt.Errorf("failed to call abi pack, err: %v", err)
	}
	return data, nil
}

func EncodeTxDAG(dag TxDAG) ([]byte, error) {
	if dag == nil {
		return nil, errors.New("input nil TxDAG")
	}
	var buf bytes.Buffer
	buf.WriteByte(dag.Type())
	if err := rlp.Encode(&buf, dag.Inner()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeTxDAG(enc []byte) (TxDAG, error) {
	if len(enc) <= 1 {
		return nil, errors.New("too short TxDAG bytes")
	}

	switch enc[0] {
	case EmptyTxDAGType:
		return NewEmptyTxDAG(), nil
	case PlainTxDAGType:
		dag := new(PlainTxDAG)
		if err := rlp.DecodeBytes(enc[1:], dag); err != nil {
			return nil, err
		}
		return dag, nil
	default:
		return nil, errors.New("unsupported TxDAG bytes")
	}
}

func ValidateTxDAG(d TxDAG, txCnt int) error {
	if d == nil {
		return nil
	}

	switch d.Type() {
	case EmptyTxDAGType:
		return nil
	case PlainTxDAGType:
		return ValidatePlainTxDAG(d, txCnt)
	default:
		return fmt.Errorf("unsupported TxDAG type: %v", d.Type())
	}
}

func ValidatePlainTxDAG(d TxDAG, txCnt int) error {
	if d.TxCount() != txCnt {
		return fmt.Errorf("PlainTxDAG contains wrong txs count, expect: %v, actual: %v", txCnt, d.TxCount())
	}
	for i := 0; i < txCnt; i++ {
		dep := d.TxDep(i)
		if dep == nil {
			return fmt.Errorf("PlainTxDAG contains nil txdep, tx: %v", i)
		}
		for j, tx := range dep.TxIndexes {
			if tx >= uint64(i) || tx >= uint64(txCnt) {
				return fmt.Errorf("PlainTxDAG contains the exceed range dependency, tx: %v", i)
			}
			if j > 0 && dep.TxIndexes[j] <= dep.TxIndexes[j-1] {
				return fmt.Errorf("PlainTxDAG contains unordered dependency, tx: %v", i)
			}
		}
		if dep.Flags != nil && *dep.Flags & ^TxDepFlagMask > 0 {
			return fmt.Errorf("PlainTxDAG contains unknown flags, flags: %v", *dep.Flags)
		}
	}
	return nil
}

// GetTxDAG return TxDAG bytes from block if there is any, or return nil if not exist
// the txDAG is stored in the calldata of the last transaction of the block
func GetTxDAG(block *Block) (TxDAG, error) {
	txs := block.Transactions()
	if txs.Len() <= 0 {
		return nil, fmt.Errorf("no txdag found")
	}
	// get data from the last tx
	return DecodeTxDAGCalldata(txs[txs.Len()-1].Data())
}

func TxDependency(d TxDAG, i int) []uint64 {
	if d == nil || i < 0 || i >= d.TxCount() {
		return []uint64{}
	}
	dep := d.TxDep(i)
	if dep.CheckFlag(ExcludedTxFlag) {
		return []uint64{}
	}
	if dep.CheckFlag(NonDependentRelFlag) {
		txs := make([]uint64, 0, d.TxCount()-dep.Count())
		for j := 0; j < i; j++ {
			if !dep.Exist(j) && j != i {
				txs = append(txs, uint64(j))
			}
		}
		return txs
	}
	return dep.TxIndexes
}

// EmptyTxDAG indicate that execute txs in sequence
// It means no transactions or need timely distribute transaction fees
// it only keep partial serial execution when tx cannot delay the distribution or just execute txs in sequence
type EmptyTxDAG struct {
}

func NewEmptyTxDAG() TxDAG {
	return &EmptyTxDAG{}
}

func (d *EmptyTxDAG) Type() byte {
	return EmptyTxDAGType
}

func (d *EmptyTxDAG) Inner() interface{} {
	return d
}

func (d *EmptyTxDAG) DelayGasFeeDistribution() bool {
	return false
}

func (d *EmptyTxDAG) TxDep(int) *TxDep {
	dep := TxDep{
		TxIndexes: nil,
		Flags:     new(uint8),
	}
	dep.SetFlag(NonDependentRelFlag)
	return &dep
}

func (d *EmptyTxDAG) TxCount() int {
	return 0
}

func (d *EmptyTxDAG) SetTxDep(int, TxDep) error {
	return nil
}

func (d *EmptyTxDAG) String() string {
	return "EmptyTxDAG"
}

// PlainTxDAG indicate how to use the dependency of txs, and delay the distribution of GasFee
type PlainTxDAG struct {
	// Tx Dependency List, the list index is equal to TxIndex
	TxDeps []TxDep
}

func (d *PlainTxDAG) Type() byte {
	return PlainTxDAGType
}

func (d *PlainTxDAG) Inner() interface{} {
	return d
}

func (d *PlainTxDAG) DelayGasFeeDistribution() bool {
	return true
}

func (d *PlainTxDAG) TxDep(i int) *TxDep {
	return &d.TxDeps[i]
}

func (d *PlainTxDAG) TxCount() int {
	return len(d.TxDeps)
}

func (d *PlainTxDAG) SetTxDep(i int, dep TxDep) error {
	if i < 0 || i > len(d.TxDeps) {
		return fmt.Errorf("SetTxDep with wrong index: %d", i)
	}
	if i < len(d.TxDeps) {
		d.TxDeps[i] = dep
		return nil
	}
	d.TxDeps = append(d.TxDeps, dep)
	return nil
}

func NewPlainTxDAG(txLen int) *PlainTxDAG {
	return &PlainTxDAG{
		TxDeps: make([]TxDep, txLen),
	}
}

func (d *PlainTxDAG) String() string {
	builder := strings.Builder{}
	for _, txDep := range d.TxDeps {
		if txDep.Flags != nil {
			builder.WriteString(fmt.Sprintf("%v|%v\n", txDep.TxIndexes, *txDep.Flags))
			continue
		}
		builder.WriteString(fmt.Sprintf("%v\n", txDep.TxIndexes))
	}
	return builder.String()
}

func (d *PlainTxDAG) Size() int {
	enc, err := EncodeTxDAG(d)
	if err != nil {
		return 0
	}
	return len(enc)
}

// MergeTxDAGExecutionPaths will merge duplicate tx path for scheduling parallel.
// Any tx cannot exist in >= 2 paths.
func MergeTxDAGExecutionPaths(d TxDAG, from, to uint64) ([][]uint64, error) {
	if from > to || to >= uint64(d.TxCount()) {
		return nil, fmt.Errorf("input wrong from: %v, to: %v, txCnt:%v", from, to, d.TxCount())
	}
	mergeMap := make(map[uint64][]uint64, d.TxCount())
	txMap := make(map[uint64]uint64, d.TxCount())
	for i := int(to); i >= int(from); i-- {
		index, merge := uint64(i), uint64(i)
		deps := TxDependency(d, i)
		// drop the out range txs
		deps = depExcludeTxRange(deps, from, to)
		if oldIdx, exist := findTxPathIndex(deps, index, txMap); exist {
			merge = oldIdx
		}
		for _, tx := range deps {
			txMap[tx] = merge
		}
		txMap[index] = merge
	}

	// result by index order
	for f, t := range txMap {
		if mergeMap[t] == nil {
			mergeMap[t] = make([]uint64, 0)
		}
		if f < from || f > to {
			continue
		}
		mergeMap[t] = append(mergeMap[t], f)
	}
	mergePaths := make([][]uint64, 0, len(mergeMap))
	for i := from; i <= to; i++ {
		path, ok := mergeMap[i]
		if !ok {
			continue
		}
		slices.Sort(path)
		mergePaths = append(mergePaths, path)
	}

	return mergePaths, nil
}

// depExcludeTxRange drop all from~to items, and deps is ordered.
func depExcludeTxRange(deps []uint64, from uint64, to uint64) []uint64 {
	if len(deps) == 0 {
		return deps
	}
	start, end := 0, len(deps)-1
	for start < len(deps) && deps[start] < from {
		start++
	}
	for end >= 0 && deps[end] > to {
		end--
	}
	if start > end {
		return nil
	}
	return deps[start : end+1]
}

func findTxPathIndex(path []uint64, cur uint64, txMap map[uint64]uint64) (uint64, bool) {
	if old, ok := txMap[cur]; ok {
		return old, true
	}

	for _, index := range path {
		if old, ok := txMap[index]; ok {
			return old, true
		}
	}

	return 0, false
}

// TxDep store the current tx dependency relation with other txs
type TxDep struct {
	TxIndexes []uint64
	// Flags may has multi flag meaning, ref NonDependentRelFlag, ExcludedTxFlag.
	Flags *uint8 `rlp:"optional"`
}

func NewTxDep(indexes []uint64, flags ...uint8) TxDep {
	dep := TxDep{
		TxIndexes: indexes,
	}
	if len(flags) == 0 {
		return dep
	}
	dep.Flags = new(uint8)
	for _, flag := range flags {
		dep.SetFlag(flag)
	}
	return dep
}

func (d *TxDep) AppendDep(i int) {
	d.TxIndexes = append(d.TxIndexes, uint64(i))
}

func (d *TxDep) Exist(i int) bool {
	for _, index := range d.TxIndexes {
		if index == uint64(i) {
			return true
		}
	}

	return false
}

func (d *TxDep) Count() int {
	return len(d.TxIndexes)
}

func (d *TxDep) Last() int {
	if d.Count() == 0 {
		return -1
	}
	return int(d.TxIndexes[len(d.TxIndexes)-1])
}

func (d *TxDep) CheckFlag(flag uint8) bool {
	var flags uint8
	if d.Flags != nil {
		flags = *d.Flags
	}
	return flags&flag == flag
}

func (d *TxDep) SetFlag(flag uint8) {
	if d.Flags == nil {
		d.Flags = new(uint8)
	}
	*d.Flags |= flag
}

func (d *TxDep) ClearFlag(flag uint8) {
	if d.Flags == nil {
		return
	}
	*d.Flags &= ^flag
}

var (
	totalTxMeter    = metrics.NewRegisteredMeter("dag/txcnt", nil)
	totalNoDepMeter = metrics.NewRegisteredMeter("dag/nodepcnt", nil)
)

func EvaluateTxDAGPerformance(dag TxDAG) {
	if dag.TxCount() == 0 {
		return
	}
	totalTxMeter.Mark(int64(dag.TxCount()))
	for i := 0; i < dag.TxCount(); i++ {
		if len(TxDependency(dag, i)) == 0 {
			totalNoDepMeter.Mark(1)
		}
	}
}
