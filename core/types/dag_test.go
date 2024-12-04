package types

import (
	"encoding/hex"
	"testing"

	"github.com/golang/snappy"

	"github.com/cometbft/cometbft/libs/rand"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeTxDAGCalldata(t *testing.T) {
	tg := mockSimpleDAG()
	data, err := EncodeTxDAGCalldata(tg)
	assert.Equal(t, nil, err)
	tg, err = DecodeTxDAGCalldata(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, tg.TxCount() > 0)

	_, err = DecodeTxDAGCalldata(nil)
	assert.NotEqual(t, nil, err)
}

func TestTxDAG_SetTxDep(t *testing.T) {
	dag := mockSimpleDAG()
	require.NoError(t, dag.SetTxDep(9, NewTxDep(nil, NonDependentRelFlag)))
	require.NoError(t, dag.SetTxDep(10, NewTxDep(nil, NonDependentRelFlag)))
	require.Error(t, dag.SetTxDep(12, NewTxDep(nil, NonDependentRelFlag)))
	dag = NewEmptyTxDAG()
	require.NoError(t, dag.SetTxDep(0, NewTxDep(nil, NonDependentRelFlag)))
	require.NoError(t, dag.SetTxDep(11, NewTxDep(nil, NonDependentRelFlag)))
}

func TestTxDAG(t *testing.T) {
	dag := mockSimpleDAG()
	t.Log(dag)
	dag = mockSystemTxDAG()
	t.Log(dag)
}

func TestEvaluateTxDAG(t *testing.T) {
	dag := mockSystemTxDAG()
	EvaluateTxDAGPerformance(dag)
}

func TestTxDAG_Compression(t *testing.T) {
	dag := mockRandomDAG(10000)
	enc, err := EncodeTxDAG(dag)
	require.NoError(t, err)
	encoded := snappy.Encode(nil, enc)
	t.Log("enc", len(enc), "compressed", len(encoded), "ratio", 1-(float64(len(encoded))/float64(len(enc))))
}

func BenchmarkTxDAG_Encode(b *testing.B) {
	dag := mockRandomDAG(10000)
	for i := 0; i < b.N; i++ {
		EncodeTxDAG(dag)
	}
}

func BenchmarkTxDAG_Decode(b *testing.B) {
	dag := mockRandomDAG(10000)
	enc, _ := EncodeTxDAG(dag)
	for i := 0; i < b.N; i++ {
		DecodeTxDAG(enc)
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
	dag.TxDeps[6].TxIndexes = []uint64{5}
	dag.TxDeps[7].TxIndexes = []uint64{6}
	dag.TxDeps[8].TxIndexes = []uint64{}
	dag.TxDeps[9].TxIndexes = []uint64{8}
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
	dag.TxDeps[6].TxIndexes = []uint64{5}
	dag.TxDeps[7].TxIndexes = []uint64{6}
	dag.TxDeps[8].TxIndexes = []uint64{}
	dag.TxDeps[9].TxIndexes = []uint64{8}
	dag.TxDeps[10] = NewTxDep([]uint64{}, ExcludedTxFlag)
	dag.TxDeps[11] = NewTxDep([]uint64{}, ExcludedTxFlag)
	return dag
}

func mockSystemTxDAG2() TxDAG {
	dag := NewPlainTxDAG(12)
	dag.TxDeps[0] = NewTxDep([]uint64{})
	dag.TxDeps[1] = NewTxDep([]uint64{})
	dag.TxDeps[2] = NewTxDep([]uint64{})
	dag.TxDeps[3] = NewTxDep([]uint64{0})
	dag.TxDeps[4] = NewTxDep([]uint64{0})
	dag.TxDeps[5] = NewTxDep([]uint64{1, 2})
	dag.TxDeps[6] = NewTxDep([]uint64{5})
	dag.TxDeps[7] = NewTxDep([]uint64{6})
	dag.TxDeps[8] = NewTxDep([]uint64{})
	dag.TxDeps[9] = NewTxDep([]uint64{8})
	dag.TxDeps[10] = NewTxDep([]uint64{}, NonDependentRelFlag)
	dag.TxDeps[11] = NewTxDep([]uint64{}, NonDependentRelFlag)
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
	dag.TxDeps[6].TxIndexes = []uint64{5}
	dag.TxDeps[7].TxIndexes = []uint64{3}
	dag.TxDeps[8].TxIndexes = []uint64{}
	//dag.TxDeps[9].TxIndexes = []uint64{0, 1, 2, 6, 7, 8}
	dag.TxDeps[9] = NewTxDep([]uint64{3, 4, 5}, NonDependentRelFlag)
	dag.TxDeps[10] = NewTxDep([]uint64{}, ExcludedTxFlag)
	dag.TxDeps[11] = NewTxDep([]uint64{}, ExcludedTxFlag)
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
		{"01cdccc280c0c280c0c280c0c280c0", true},
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

func TestTxDep_Flags(t *testing.T) {
	dep := NewTxDep(nil)
	dep.ClearFlag(NonDependentRelFlag)
	dep.SetFlag(NonDependentRelFlag)
	dep.SetFlag(ExcludedTxFlag)
	compared := NewTxDep(nil, NonDependentRelFlag, ExcludedTxFlag)
	require.Equal(t, dep, compared)
	require.Equal(t, NonDependentRelFlag|ExcludedTxFlag, *dep.Flags)
	dep.ClearFlag(ExcludedTxFlag)
	require.Equal(t, NonDependentRelFlag, *dep.Flags)
	require.True(t, dep.CheckFlag(NonDependentRelFlag))
	require.False(t, dep.CheckFlag(ExcludedTxFlag))
}
