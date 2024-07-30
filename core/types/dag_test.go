package types

import (
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

var (
	mockAddr = common.HexToAddress("0x482bA86399ab6Dcbe54071f8d22258688B4509b1")
	mockHash = common.HexToHash("0xdc13f8d7bdb8ec4de02cd4a50a1aa2ab73ec8814e0cdb550341623be3dd8ab7a")
)

func TestTxDAG(t *testing.T) {
	dag := mockSimpleDAG()
	require.NoError(t, dag.SetTxDep(9, TxDep{
		Relation:  1,
		TxIndexes: nil,
	}))
	require.NoError(t, dag.SetTxDep(10, TxDep{
		Relation:  1,
		TxIndexes: nil,
	}))
	require.Error(t, dag.SetTxDep(12, TxDep{
		Relation:  1,
		TxIndexes: nil,
	}))
	dag = NewEmptyTxDAG()
	require.NoError(t, dag.SetTxDep(0, TxDep{
		Relation:  1,
		TxIndexes: nil,
	}))
	require.NoError(t, dag.SetTxDep(11, TxDep{
		Relation:  1,
		TxIndexes: nil,
	}))
}

func TestTxDAG_SetTxDep(t *testing.T) {
	dag := mockSimpleDAG()
	t.Log(dag)
	dag = mockSystemTxDAG()
	t.Log(dag)
}

func TestEvaluateTxDAG(t *testing.T) {
	dag := mockSystemTxDAG()
	stats := make(map[int]*ExeStat, dag.TxCount())
	for i := 0; i < dag.TxCount(); i++ {
		stats[i] = NewExeStat(i).WithGas(uint64(i)).WithRead(i)
		stats[i].costTime = time.Duration(i)
		if dag.TxDep(i).Relation == 1 {
			stats[i].WithSerialFlag()
		}
	}
	EvaluateTxDAGPerformance(dag, stats)
}

func TestMergeTxDAGExecutionPaths_Simple(t *testing.T) {
	paths := MergeTxDAGExecutionPaths(mockSimpleDAG())
	require.Equal(t, [][]uint64{
		{0, 3, 4},
		{1, 2, 5, 6, 7},
		{8, 9},
	}, paths)
}

func TestMergeTxDAGExecutionPaths_Random(t *testing.T) {
	dag := mockRandomDAG(10000)
	paths := MergeTxDAGExecutionPaths(dag)
	txMap := make(map[uint64]uint64, dag.TxCount())
	for _, path := range paths {
		for _, index := range path {
			old, ok := txMap[index]
			require.False(t, ok, index, path, old)
			txMap[index] = path[0]
		}
	}
	require.Equal(t, dag.TxCount(), len(txMap))
}

func BenchmarkMergeTxDAGExecutionPaths(b *testing.B) {
	dag := mockRandomDAG(100000)
	for i := 0; i < b.N; i++ {
		MergeTxDAGExecutionPaths(dag)
	}
}

func mockSimpleDAG() TxDAG {
	dag := NewPlainTxDAG(10)
	dag.TxDeps[0].TxIndexes = []uint64{}
	dag.TxDeps[1].TxIndexes = []uint64{}
	dag.TxDeps[2].TxIndexes = []uint64{}
	dag.TxDeps[3].TxIndexes = []uint64{0}
	dag.TxDeps[4].TxIndexes = []uint64{0}
	dag.TxDeps[5].TxIndexes = []uint64{1, 2}
	dag.TxDeps[6].TxIndexes = []uint64{2, 5}
	dag.TxDeps[7].TxIndexes = []uint64{6}
	dag.TxDeps[8].TxIndexes = []uint64{}
	dag.TxDeps[9].TxIndexes = []uint64{8}
	return dag
}

func mockRandomDAG(txLen int) TxDAG {
	dag := NewPlainTxDAG(txLen)
	for i := 0; i < txLen; i++ {
		var deps []uint64
		if i == 0 || rand.Bool() {
			dag.TxDeps[i].TxIndexes = deps
			continue
		}
		depCnt := rand.Int()%i + 1
		for j := 0; j < depCnt; j++ {
			var dep uint64
			if j > 0 && deps[j-1]+1 == uint64(i) {
				break
			}
			if j > 0 {
				dep = uint64(rand.Int())%(uint64(i)-deps[j-1]-1) + deps[j-1] + 1
			} else {
				dep = uint64(rand.Int() % i)
			}
			deps = append(deps, dep)
		}
		dag.TxDeps[i].TxIndexes = deps
	}
	return dag
}

func mockSystemTxDAG() TxDAG {
	dag := NewPlainTxDAG(12)
	dag.TxDeps[0].TxIndexes = []uint64{}
	dag.TxDeps[1].TxIndexes = []uint64{}
	dag.TxDeps[2].TxIndexes = []uint64{}
	dag.TxDeps[3].TxIndexes = []uint64{0}
	dag.TxDeps[4].TxIndexes = []uint64{0}
	dag.TxDeps[5].TxIndexes = []uint64{1, 2}
	dag.TxDeps[6].TxIndexes = []uint64{2, 5}
	dag.TxDeps[7].TxIndexes = []uint64{6}
	dag.TxDeps[8].TxIndexes = []uint64{}
	dag.TxDeps[9].TxIndexes = []uint64{8}
	dag.TxDeps[10] = TxDep{
		Relation:  1,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[11] = TxDep{
		Relation:  1,
		TxIndexes: []uint64{},
	}
	return dag
}

func TestTxDAG_Encode_Decode(t *testing.T) {
	expected := TxDAG(&EmptyTxDAG{})
	enc, err := EncodeTxDAG(expected)
	require.NoError(t, err)
	actual, err := DecodeTxDAG(enc)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	expected = mockSimpleDAG()
	enc, err = EncodeTxDAG(expected)
	require.NoError(t, err)
	actual, err = DecodeTxDAG(enc)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	enc[0] = 2
	_, err = DecodeTxDAG(enc)
	require.Error(t, err)
}
