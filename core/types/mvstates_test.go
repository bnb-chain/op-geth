package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestMVStates_BasicUsage(t *testing.T) {
	ms := NewMVStates(0)
	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(0, []interface{}{"0x00", 0}, []interface{}{"0x00", 0}), nil))
	require.Nil(t, ms.ReadState(0, str2key("0x00")))
	require.NoError(t, ms.Finalise(0))
	require.Error(t, ms.Finalise(0))
	require.Error(t, ms.FulfillRWSet(mockRWSetWithVal(0, nil, nil), nil))
	require.Nil(t, ms.ReadState(0, str2key("0x00")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 0}, 0), ms.ReadState(1, str2key("0x00")))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(1, []interface{}{"0x01", 1}, []interface{}{"0x01", 1}), nil))
	require.Nil(t, ms.ReadState(1, str2key("0x01")))
	require.NoError(t, ms.Finalise(1))
	require.Nil(t, ms.ReadState(0, str2key("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadState(2, str2key("0x01")))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(2, []interface{}{"0x02", 2, "0x01", 1}, []interface{}{"0x01", 2, "0x02", 2}), nil))
	require.NoError(t, ms.Finalise(2))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadState(2, str2key("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadState(3, str2key("0x01")))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(3, []interface{}{"0x03", 3, "0x00", 0, "0x01", 2}, []interface{}{"0x00", 3, "0x01", 3, "0x03", 3}), nil))
	require.Nil(t, ms.ReadState(3, str2key("0x03")))
	require.NoError(t, ms.Finalise(3))
	require.Nil(t, ms.ReadState(0, str2key("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadState(2, str2key("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadState(3, str2key("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadState(4, str2key("0x01")))
	require.Nil(t, ms.ReadState(0, str2key("0x00")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadState(5, str2key("0x00")))
}

func TestMVStates_SimpleResolveTxDAG(t *testing.T) {
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

	dag, err := ms.ResolveTxDAG(10, nil)
	require.NoError(t, err)
	require.Equal(t, mockSimpleDAG(), dag)
	t.Log(dag)
}

func TestMVStates_SystemTxResolveTxDAG(t *testing.T) {
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
	ms.rwSets[10] = mockRWSet(10, []string{"0x10"}, []string{"0x10"}).WithExcludedTxFlag()
	ms.rwSets[11] = mockRWSet(11, []string{"0x11"}, []string{"0x11"}).WithExcludedTxFlag()

	dag, err := ms.ResolveTxDAG(12, nil)
	require.NoError(t, err)
	require.Equal(t, mockSystemTxDAG(), dag)
	t.Log(dag)
}

func TestMVStates_SystemTxWithLargeDepsResolveTxDAG(t *testing.T) {
	ms := NewMVStates(12)

	ms.rwSets[0] = mockRWSet(0, []string{"0x00"}, []string{"0x00"})
	ms.rwSets[1] = mockRWSet(1, []string{"0x01"}, []string{"0x01"})
	ms.rwSets[2] = mockRWSet(2, []string{"0x02"}, []string{"0x02"})
	ms.rwSets[3] = mockRWSet(3, []string{"0x00", "0x03"}, []string{"0x03"})
	ms.rwSets[4] = mockRWSet(4, []string{"0x00", "0x04"}, []string{"0x04"})
	ms.rwSets[5] = mockRWSet(5, []string{"0x01", "0x02", "0x05"}, []string{"0x05"})
	ms.rwSets[6] = mockRWSet(6, []string{"0x02", "0x05", "0x06"}, []string{"0x06"})
	ms.rwSets[7] = mockRWSet(7, []string{"0x00", "0x01", "0x03", "0x05", "0x06", "0x07"}, []string{"0x07"})
	ms.rwSets[8] = mockRWSet(8, []string{"0x08"}, []string{"0x08"})
	ms.rwSets[9] = mockRWSet(9, []string{"0x00", "0x01", "0x02", "0x03", "0x04", "0x08", "0x09"}, []string{"0x09"})
	ms.rwSets[10] = mockRWSet(10, []string{"0x10"}, []string{"0x10"}).WithExcludedTxFlag()
	ms.rwSets[11] = mockRWSet(11, []string{"0x11"}, []string{"0x11"}).WithExcludedTxFlag()

	dag, err := ms.ResolveTxDAG(12, nil)
	require.NoError(t, err)
	require.Equal(t, mockSystemTxDAGWithLargeDeps(), dag)
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

func mockRWSet(index int, read []string, write []string) *RWSet {
	ver := StateVersion{
		TxIndex: index,
	}
	set := NewRWSet(ver)
	for _, k := range read {
		set.readSet[str2key(k)] = &RWItem{
			Ver: ver,
			Val: struct{}{},
		}
	}
	for _, k := range write {
		set.writeSet[str2key(k)] = &RWItem{
			Ver: ver,
			Val: struct{}{},
		}
	}

	return set
}

func mockRWSetWithVal(index int, read []interface{}, write []interface{}) *RWSet {
	ver := StateVersion{
		TxIndex: index,
	}
	set := NewRWSet(ver)

	if len(read)%2 != 0 {
		panic("wrong read size")
	}
	if len(write)%2 != 0 {
		panic("wrong write size")
	}

	for i := 0; i < len(read); {
		set.readSet[str2key(read[i].(string))] = &RWItem{
			Ver: StateVersion{
				TxIndex: index - 1,
			},
			Val: read[i+1],
		}
		i += 2
	}
	for i := 0; i < len(write); {
		set.writeSet[str2key(write[i].(string))] = &RWItem{
			Ver: ver,
			Val: write[i+1],
		}
		i += 2
	}

	return set
}

func str2key(k string) RWKey {
	key := RWKey{}
	if len(k) > len(key) {
		k = k[:len(key)]
	}
	copy(key[:], k)
	return key
}
