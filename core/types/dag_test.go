package types

import (
	"encoding/hex"
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

func TestTxDAG_SetTxDep(t *testing.T) {
	dag := mockSimpleDAG()
	require.NoError(t, dag.SetTxDep(9, TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: nil,
	}))
	require.NoError(t, dag.SetTxDep(10, TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: nil,
	}))
	require.Error(t, dag.SetTxDep(12, TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: nil,
	}))
	dag = NewEmptyTxDAG()
	require.NoError(t, dag.SetTxDep(0, TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: nil,
	}))
	require.NoError(t, dag.SetTxDep(11, TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: nil,
	}))
}

func TestTxDAG(t *testing.T) {
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
		txDep := dag.TxDep(i)
		if txDep.RelationEqual(TxDAGRelation1) {
			stats[i].WithSerialFlag()
		}
	}
	EvaluateTxDAGPerformance(dag, stats)
}

func TestMergeTxDAGExecutionPaths_Simple(t *testing.T) {
	tests := []struct {
		d      TxDAG
		expect [][]uint64
	}{
		{
			d: mockSimpleDAG(),
			expect: [][]uint64{
				{0, 3, 4},
				{1, 2, 5, 6, 7},
				{8, 9},
			},
		},
		{
			d: mockSimpleDAGWithLargeDeps(),
			expect: [][]uint64{
				{5, 6},
				{0, 1, 2, 3, 4, 7, 8, 9},
			},
		},
		{
			d: mockSystemTxDAGWithLargeDeps(),
			expect: [][]uint64{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
		},
	}
	for i, item := range tests {
		paths := MergeTxDAGExecutionPaths(item.d)
		require.Equal(t, item.expect, paths, i)
	}
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

func mockSimpleDAGWithLargeDeps() TxDAG {
	dag := NewPlainTxDAG(10)
	dag.TxDeps[0].TxIndexes = []uint64{}
	dag.TxDeps[1].TxIndexes = []uint64{}
	dag.TxDeps[2].TxIndexes = []uint64{}
	dag.TxDeps[3].TxIndexes = []uint64{0}
	dag.TxDeps[4].TxIndexes = []uint64{0}
	dag.TxDeps[5].TxIndexes = []uint64{}
	dag.TxDeps[6].TxIndexes = []uint64{5}
	dag.TxDeps[7].TxIndexes = []uint64{2, 4}
	dag.TxDeps[8].TxIndexes = []uint64{}
	//dag.TxDeps[9].TxIndexes = []uint64{0, 1, 3, 4, 8}
	dag.TxDeps[9] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{2, 5, 6, 7},
	}
	return dag
}

func mockRandomDAG(txLen int) TxDAG {
	dag := NewPlainTxDAG(txLen)
	for i := 0; i < txLen; i++ {
		deps := make([]uint64, 0)
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
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[11] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	return dag
}

func mockSystemTxDAG2() TxDAG {
	dag := NewPlainTxDAG(12)
	dag.TxDeps[0] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[1] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[2] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[3] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{0},
	}
	dag.TxDeps[4] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{0},
	}
	dag.TxDeps[5] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{1, 2},
	}
	dag.TxDeps[6] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{2, 5},
	}
	dag.TxDeps[7] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{6},
	}
	dag.TxDeps[8] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[9] = TxDep{
		Relation:  &TxDAGRelation0,
		TxIndexes: []uint64{8},
	}
	dag.TxDeps[10] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[11] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	return dag
}

func mockSystemTxDAGWithLargeDeps() TxDAG {
	dag := NewPlainTxDAG(12)
	dag.TxDeps[0].TxIndexes = []uint64{}
	dag.TxDeps[1].TxIndexes = []uint64{}
	dag.TxDeps[2].TxIndexes = []uint64{}
	dag.TxDeps[3].TxIndexes = []uint64{0}
	dag.TxDeps[4].TxIndexes = []uint64{0}
	dag.TxDeps[5].TxIndexes = []uint64{1, 2}
	dag.TxDeps[6].TxIndexes = []uint64{2, 5}
	dag.TxDeps[7].TxIndexes = []uint64{0, 1, 3, 5, 6}
	dag.TxDeps[8].TxIndexes = []uint64{}
	//dag.TxDeps[9].TxIndexes = []uint64{0, 1, 2, 3, 4, 8}
	dag.TxDeps[9] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{5, 6, 7, 10, 11},
	}
	dag.TxDeps[10] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	dag.TxDeps[11] = TxDep{
		Relation:  &TxDAGRelation1,
		TxIndexes: []uint64{},
	}
	return dag
}

func TestTxDAG_Encode_Decode(t *testing.T) {
	tests := []struct {
		expect TxDAG
	}{
		{
			expect: TxDAG(&EmptyTxDAG{}),
		},
		{
			expect: mockSimpleDAG(),
		},
		{
			expect: mockRandomDAG(100),
		},
		{
			expect: mockSystemTxDAG(),
		},
		{
			expect: mockSystemTxDAG2(),
		},
		{
			expect: mockSystemTxDAGWithLargeDeps(),
		},
	}
	for i, item := range tests {
		enc, err := EncodeTxDAG(item.expect)
		t.Log(hex.EncodeToString(enc))
		require.NoError(t, err, i)
		actual, err := DecodeTxDAG(enc)
		require.NoError(t, err, i)
		require.Equal(t, item.expect, actual, i)
		if i%2 == 0 {
			enc[0] = 2
			_, err = DecodeTxDAG(enc)
			require.Error(t, err)
		}
	}
}

func TestDecodeTxDAG(t *testing.T) {
	tests := []struct {
		enc string
		err bool
	}{
		{"00c0", false},
		{"01dddcc1c0c1c0c1c0c2c180c2c180c3c20102c3c20205c2c106c1c0c2c108", false},
		{"01e3e2c1c0c1c0c1c0c2c180c2c180c3c20102c3c20205c2c106c1c0c2c108c2c001c2c001", false},
		{"0132e212", true},
		{"01dfdec280c0c280c0c380c101c380c102c380c103c380c104c380c105c380c106", true},
	}
	for i, item := range tests {
		enc, err := hex.DecodeString(item.enc)
		require.NoError(t, err, i)
		txDAG, err := DecodeTxDAG(enc)
		if item.err {
			require.Error(t, err, i)
			continue
		}
		require.NoError(t, err, i)
		t.Log(txDAG)
	}
}
