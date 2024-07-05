package types

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

var (
	mockAddr = common.HexToAddress("0x482bA86399ab6Dcbe54071f8d22258688B4509b1")
	mockHash = common.HexToHash("0xdc13f8d7bdb8ec4de02cd4a50a1aa2ab73ec8814e0cdb550341623be3dd8ab7a")
)

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
		if dag.TxDep(i).Relation == 1 {
			stats[i].WithSerialFlag()
		}
	}
	t.Log(EvaluateTxDAGPerformance(dag, stats))
}

func TestSimpleMVStates2TxDAG(t *testing.T) {
	ms := NewMVStates(10)

	ms.rwSets[0] = mockRWSet(0, []string{"0x00"}, []string{"0x00"})
	ms.rwSets[1] = mockRWSet(1, []string{"0x01"}, []string{"0x01"})
	ms.rwSets[2] = mockRWSet(2, []string{"0x02"}, []string{"0x02"})
	ms.rwSets[3] = mockRWSet(3, []string{"0x00", "0x03"}, []string{"0x03"})
	ms.rwSets[4] = mockRWSet(4, []string{"0x00", "0x04"}, []string{"0x04"})
	ms.rwSets[5] = mockRWSet(5, []string{"0x01", "0x02", "0x05"}, []string{"0x05"})
	ms.rwSets[6] = mockRWSet(6, []string{"0x02", "0x05", "0x06"}, []string{"0x06"})
	ms.rwSets[7] = mockRWSet(7, []string{"0x06", "0x07"}, []string{"0x07"})
	ms.rwSets[8] = mockRWSet(8, []string{"0x08"}, []string{"0x08"})
	ms.rwSets[9] = mockRWSet(9, []string{"0x08", "0x09"}, []string{"0x09"})

	dag := ms.ResolveTxDAG()
	require.Equal(t, mockSimpleDAG(), dag)
	t.Log(dag)
}

func TestSystemTxMVStates2TxDAG(t *testing.T) {
	ms := NewMVStates(12)

	ms.rwSets[0] = mockRWSet(0, []string{"0x00"}, []string{"0x00"})
	ms.rwSets[1] = mockRWSet(1, []string{"0x01"}, []string{"0x01"})
	ms.rwSets[2] = mockRWSet(2, []string{"0x02"}, []string{"0x02"})
	ms.rwSets[3] = mockRWSet(3, []string{"0x00", "0x03"}, []string{"0x03"})
	ms.rwSets[4] = mockRWSet(4, []string{"0x00", "0x04"}, []string{"0x04"})
	ms.rwSets[5] = mockRWSet(5, []string{"0x01", "0x02", "0x05"}, []string{"0x05"})
	ms.rwSets[6] = mockRWSet(6, []string{"0x02", "0x05", "0x06"}, []string{"0x06"})
	ms.rwSets[7] = mockRWSet(7, []string{"0x06", "0x07"}, []string{"0x07"})
	ms.rwSets[8] = mockRWSet(8, []string{"0x08"}, []string{"0x08"})
	ms.rwSets[9] = mockRWSet(9, []string{"0x08", "0x09"}, []string{"0x09"})
	ms.rwSets[10] = mockRWSet(10, []string{"0x10"}, []string{"0x10"}).WithSerialFlag()
	ms.rwSets[11] = mockRWSet(11, []string{"0x11"}, []string{"0x11"}).WithSerialFlag()

	dag := ms.ResolveTxDAG()
	require.Equal(t, mockSystemTxDAG(), dag)
	t.Log(dag)
}

func TestIsEqualRWVal(t *testing.T) {
	tests := []struct {
		key      RWKey
		src      interface{}
		compared interface{}
		isEqual  bool
	}{
		{
			key:      AccountStateKey(mockAddr, AccountNonce),
			src:      uint64(0),
			compared: uint64(0),
			isEqual:  true,
		},
		{
			key:      AccountStateKey(mockAddr, AccountNonce),
			src:      uint64(0),
			compared: uint64(1),
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountBalance),
			src:      new(uint256.Int).SetUint64(1),
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  true,
		},
		{
			key:      AccountStateKey(mockAddr, AccountBalance),
			src:      nil,
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountBalance),
			src:      (*uint256.Int)(nil),
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountBalance),
			src:      (*uint256.Int)(nil),
			compared: (*uint256.Int)(nil),
			isEqual:  true,
		},
		{
			key:      AccountStateKey(mockAddr, AccountCodeHash),
			src:      []byte{1},
			compared: []byte{1},
			isEqual:  true,
		},
		{
			key:      AccountStateKey(mockAddr, AccountCodeHash),
			src:      nil,
			compared: []byte{1},
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountCodeHash),
			src:      ([]byte)(nil),
			compared: []byte{1},
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountCodeHash),
			src:      ([]byte)(nil),
			compared: ([]byte)(nil),
			isEqual:  true,
		},
		{
			key:      AccountStateKey(mockAddr, AccountSuicide),
			src:      struct{}{},
			compared: struct{}{},
			isEqual:  false,
		},
		{
			key:      AccountStateKey(mockAddr, AccountSuicide),
			src:      nil,
			compared: struct{}{},
			isEqual:  false,
		},
		{
			key:      StorageStateKey(mockAddr, mockHash),
			src:      mockHash,
			compared: mockHash,
			isEqual:  true,
		},
		{
			key:      StorageStateKey(mockAddr, mockHash),
			src:      nil,
			compared: mockHash,
			isEqual:  false,
		},
	}

	for i, item := range tests {
		require.Equal(t, item.isEqual, isEqualRWVal(item.key, item.src, item.compared), i)
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

func mockRWSet(index int, read []string, write []string) *RWSet {
	ver := StateVersion{
		TxIndex: index,
	}
	set := NewRWSet(ver)
	for _, k := range read {
		key := RWKey{}
		if len(k) > len(key) {
			k = k[:len(key)]
		}
		copy(key[:], k)
		set.readSet[key] = &ReadRecord{
			StateVersion: ver,
			Val:          struct{}{},
		}
	}
	for _, k := range write {
		key := RWKey{}
		if len(k) > len(key) {
			k = k[:len(key)]
		}
		copy(key[:], k)
		set.writeSet[key] = &WriteRecord{
			Val: struct{}{},
		}
	}

	return set
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
