package types

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/cometbft/cometbft/libs/rand"
	"github.com/golang/snappy"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

const (
	mockRWSetSize = 5000
)

func TestMVStates_BasicUsage(t *testing.T) {
	ms := NewMVStates(0)
	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(0, []interface{}{AccountSelf, nil, AccountBalance, 0, "0x00", 0}, []interface{}{AccountBalance, 0, "0x00", 0}), nil))
	require.Nil(t, ms.ReadAccState(0, common.Address{}, AccountBalance))
	require.Nil(t, ms.ReadSlotState(0, common.Address{}, str2Slot("0x00")))
	require.NoError(t, ms.Finalise(0))

	require.Nil(t, ms.ReadAccState(0, common.Address{}, AccountBalance))
	require.Nil(t, ms.ReadSlotState(0, common.Address{}, str2Slot("0x00")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 0}, 0), ms.ReadAccState(1, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 0}, 0), ms.ReadSlotState(1, common.Address{}, str2Slot("0x00")))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(1, []interface{}{AccountSelf, nil, AccountBalance, 0, "0x01", 1}, []interface{}{AccountBalance, 1, "0x01", 1}), nil))
	require.Nil(t, ms.ReadSlotState(1, common.Address{}, str2Slot("0x01")))
	require.NoError(t, ms.Finalise(1))
	require.Nil(t, ms.ReadSlotState(0, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadSlotState(2, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadAccState(2, common.Address{}, AccountBalance))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(2, []interface{}{AccountSelf, nil, AccountBalance, 1, "0x02", 2, "0x01", 1}, []interface{}{AccountBalance, 2, "0x01", 2, "0x02", 2}), nil))
	require.NoError(t, ms.Finalise(2))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadAccState(2, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadSlotState(2, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadAccState(3, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadSlotState(3, common.Address{}, str2Slot("0x01")))

	require.NoError(t, ms.FulfillRWSet(mockRWSetWithVal(3, []interface{}{AccountSelf, nil, AccountBalance, 2, "0x03", 3, "0x00", 0, "0x01", 2}, []interface{}{AccountBalance, 3, "0x00", 3, "0x01", 3, "0x03", 3}), nil))
	require.Nil(t, ms.ReadSlotState(3, common.Address{}, str2Slot("0x03")))
	require.NoError(t, ms.Finalise(3))
	require.Nil(t, ms.ReadSlotState(0, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadSlotState(2, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 1}, 1), ms.ReadAccState(2, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadSlotState(3, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 2}, 2), ms.ReadAccState(3, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadSlotState(4, common.Address{}, str2Slot("0x01")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadAccState(4, common.Address{}, AccountBalance))
	require.Nil(t, ms.ReadSlotState(0, common.Address{}, str2Slot("0x00")))
	require.Nil(t, ms.ReadAccState(0, common.Address{}, AccountBalance))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadSlotState(5, common.Address{}, str2Slot("0x00")))
	require.Equal(t, NewRWItem(StateVersion{TxIndex: 3}, 3), ms.ReadAccState(5, common.Address{}, AccountBalance))
}

func TestMVStates_SimpleResolveTxDAG(t *testing.T) {
	ms := NewMVStates(10)
	finaliseRWSets(t, ms, []*RWSet{
		mockRWSet(0, []interface{}{"0x00"}, []interface{}{"0x00"}),
		mockRWSet(1, []interface{}{"0x01"}, []interface{}{"0x01"}),
		mockRWSet(2, []interface{}{"0x02"}, []interface{}{"0x02"}),
		mockRWSet(3, []interface{}{"0x00", "0x03"}, []interface{}{"0x03"}),
		mockRWSet(4, []interface{}{"0x00", "0x04"}, []interface{}{"0x04"}),
		mockRWSet(5, []interface{}{"0x01", "0x02", "0x05"}, []interface{}{"0x05"}),
		mockRWSet(6, []interface{}{"0x02", "0x05", "0x06"}, []interface{}{"0x06"}),
		mockRWSet(7, []interface{}{"0x06", "0x07"}, []interface{}{"0x07"}),
		mockRWSet(8, []interface{}{"0x08"}, []interface{}{"0x08"}),
		mockRWSet(9, []interface{}{"0x08", "0x09"}, []interface{}{"0x09"}),
	})

	dag, err := ms.ResolveTxDAG(10, nil)
	require.NoError(t, err)
	require.Equal(t, mockSimpleDAG(), dag)
	t.Log(dag)
}

func TestMVStates_AsyncDepGen_SimpleResolveTxDAG(t *testing.T) {
	ms := NewMVStates(10).EnableAsyncGen()
	finaliseRWSets(t, ms, []*RWSet{
		mockRWSet(0, []interface{}{"0x00"}, []interface{}{"0x00"}),
		mockRWSet(1, []interface{}{"0x01"}, []interface{}{"0x01"}),
		mockRWSet(2, []interface{}{"0x02"}, []interface{}{"0x02"}),
		mockRWSet(3, []interface{}{"0x00", "0x03"}, []interface{}{"0x03"}),
		mockRWSet(4, []interface{}{"0x00", "0x04"}, []interface{}{"0x04"}),
		mockRWSet(5, []interface{}{"0x01", "0x02", "0x05"}, []interface{}{"0x05"}),
		mockRWSet(6, []interface{}{"0x02", "0x05", "0x06"}, []interface{}{"0x06"}),
		mockRWSet(7, []interface{}{"0x06", "0x07"}, []interface{}{"0x07"}),
		mockRWSet(8, []interface{}{"0x08"}, []interface{}{"0x08"}),
		mockRWSet(9, []interface{}{"0x08", "0x09"}, []interface{}{"0x09"}),
	})
	time.Sleep(10 * time.Millisecond)

	dag, err := ms.ResolveTxDAG(10, nil)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, ms.Stop())
	require.Equal(t, mockSimpleDAG(), dag)
	t.Log(dag)
}

func TestMVStates_ResolveTxDAG_Async(t *testing.T) {
	txCnt := 10000
	rwSets := mockRandomRWSet(txCnt)
	ms1 := NewMVStates(txCnt).EnableAsyncGen()
	for i := 0; i < txCnt; i++ {
		require.NoError(t, ms1.FulfillRWSet(rwSets[i], nil))
		require.NoError(t, ms1.Finalise(i))
	}
	time.Sleep(100 * time.Millisecond)
	_, err := ms1.ResolveTxDAG(txCnt, nil)
	require.NoError(t, err)
}

func TestMVStates_ResolveTxDAG_Compare(t *testing.T) {
	txCnt := 3000
	rwSets := mockRandomRWSet(txCnt)
	ms1 := NewMVStates(txCnt)
	ms2 := NewMVStates(txCnt)
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
		ms2.rwSets[i] = rwSet
		require.NoError(t, ms2.Finalise(i))
	}

	d1 := resolveTxDAGInMVStates(ms1)
	d2 := resolveDepsMapCacheByWritesInMVStates(ms2)
	require.Equal(t, d1.(*PlainTxDAG).String(), d2.(*PlainTxDAG).String())
}

func TestMVStates_TxDAG_Compression(t *testing.T) {
	txCnt := 10000
	rwSets := mockRandomRWSet(txCnt)
	ms1 := NewMVStates(txCnt)
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
		ms1.Finalise(i)
	}
	dag := resolveDepsMapCacheByWritesInMVStates(ms1)
	enc, err := EncodeTxDAG(dag)
	require.NoError(t, err)

	// snappy compression
	start := time.Now()
	encoded := snappy.Encode(nil, enc)
	t.Log("snappy", "enc", len(enc), "compressed", len(encoded),
		"ratio", 1-(float64(len(encoded))/float64(len(enc))),
		"time", float64(time.Since(start).Microseconds())/1000)

	// gzip compression
	start = time.Now()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err = zw.Write(enc)
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)
	encoded = buf.Bytes()
	t.Log("gzip", "enc", len(enc), "compressed", len(encoded),
		"ratio", 1-(float64(len(encoded))/float64(len(enc))),
		"time", float64(time.Since(start).Microseconds())/1000)
}

func BenchmarkResolveTxDAGInMVStates(b *testing.B) {
	rwSets := mockRandomRWSet(mockRWSetSize)
	ms1 := NewMVStates(mockRWSetSize)
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolveTxDAGInMVStates(ms1)
	}
}

func BenchmarkResolveTxDAGByWritesInMVStates(b *testing.B) {
	rwSets := mockRandomRWSet(mockRWSetSize)
	ms1 := NewMVStates(mockRWSetSize)
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
		ms1.innerFinalise(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolveDepsMapCacheByWritesInMVStates(ms1)
	}
}

func BenchmarkMVStates_Finalise(b *testing.B) {
	rwSets := mockRandomRWSet(mockRWSetSize)
	ms1 := NewMVStates(mockRWSetSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, rwSet := range rwSets {
			ms1.rwSets[k] = rwSet
			ms1.innerFinalise(k)
		}
	}
}

func resolveTxDAGInMVStates(s *MVStates) TxDAG {
	txDAG := NewPlainTxDAG(len(s.rwSets))
	for i := 0; i < len(s.rwSets); i++ {
		s.resolveDepsCache(i, s.rwSets[i])
		txDAG.TxDeps[i].TxIndexes = s.txDepCache[i].deps()
	}
	return txDAG
}

func resolveDepsMapCacheByWritesInMVStates(s *MVStates) TxDAG {
	txDAG := NewPlainTxDAG(len(s.rwSets))
	for i := 0; i < len(s.rwSets); i++ {
		s.resolveDepsMapCacheByWrites(i, s.rwSets[i])
		txDAG.TxDeps[i].TxIndexes = s.txDepCache[i].deps()
	}
	return txDAG
}

func TestMVStates_SystemTxResolveTxDAG(t *testing.T) {
	ms := NewMVStates(12)
	finaliseRWSets(t, ms, []*RWSet{
		mockRWSet(0, []interface{}{"0x00"}, []interface{}{"0x00"}),
		mockRWSet(1, []interface{}{"0x01"}, []interface{}{"0x01"}),
		mockRWSet(2, []interface{}{"0x02"}, []interface{}{"0x02"}),
		mockRWSet(3, []interface{}{"0x00", "0x03"}, []interface{}{"0x03"}),
		mockRWSet(4, []interface{}{"0x00", "0x04"}, []interface{}{"0x04"}),
		mockRWSet(5, []interface{}{"0x01", "0x02", "0x05"}, []interface{}{"0x05"}),
		mockRWSet(6, []interface{}{"0x02", "0x05", "0x06"}, []interface{}{"0x06"}),
		mockRWSet(7, []interface{}{"0x06", "0x07"}, []interface{}{"0x07"}),
		mockRWSet(8, []interface{}{"0x08"}, []interface{}{"0x08"}),
		mockRWSet(9, []interface{}{"0x08", "0x09"}, []interface{}{"0x09"}),
		mockRWSet(10, []interface{}{"0x10"}, []interface{}{"0x10"}).WithExcludedTxFlag(),
		mockRWSet(11, []interface{}{"0x11"}, []interface{}{"0x11"}).WithExcludedTxFlag(),
	})

	dag, err := ms.ResolveTxDAG(12, nil)
	require.NoError(t, err)
	require.Equal(t, mockSystemTxDAG(), dag)
	t.Log(dag)
}

func TestMVStates_SystemTxWithLargeDepsResolveTxDAG(t *testing.T) {
	ms := NewMVStates(12)
	finaliseRWSets(t, ms, []*RWSet{
		mockRWSet(0, []interface{}{"0x00"}, []interface{}{"0x00"}),
		mockRWSet(1, []interface{}{"0x01"}, []interface{}{"0x01"}),
		mockRWSet(2, []interface{}{"0x02"}, []interface{}{"0x02"}),
		mockRWSet(3, []interface{}{"0x00", "0x03"}, []interface{}{"0x03"}),
		mockRWSet(4, []interface{}{"0x00", "0x04"}, []interface{}{"0x04"}),
		mockRWSet(5, []interface{}{"0x01", "0x02", "0x05"}, []interface{}{"0x05"}),
		mockRWSet(6, []interface{}{"0x02", "0x05", "0x06"}, []interface{}{"0x06"}),
		mockRWSet(7, []interface{}{"0x00", "0x03", "0x07"}, []interface{}{"0x07"}),
		mockRWSet(8, []interface{}{"0x08"}, []interface{}{"0x08"}),
		mockRWSet(9, []interface{}{"0x00", "0x01", "0x02", "0x06", "0x07", "0x08", "0x09"}, []interface{}{"0x09"}),
		mockRWSet(10, []interface{}{"0x10"}, []interface{}{"0x10"}).WithExcludedTxFlag(),
		mockRWSet(11, []interface{}{"0x11"}, []interface{}{"0x11"}).WithExcludedTxFlag(),
	})
	dag, err := ms.ResolveTxDAG(12, nil)
	require.NoError(t, err)
	require.Equal(t, mockSystemTxDAGWithLargeDeps(), dag)
	t.Log(dag)
}

func TestIsEqualRWVal(t *testing.T) {
	tests := []struct {
		key      *AccountState
		src      interface{}
		compared interface{}
		isEqual  bool
	}{
		{
			key:      &AccountNonce,
			src:      uint64(0),
			compared: uint64(0),
			isEqual:  true,
		},
		{
			key:      &AccountNonce,
			src:      uint64(0),
			compared: uint64(1),
			isEqual:  false,
		},
		{
			key:      &AccountBalance,
			src:      new(uint256.Int).SetUint64(1),
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  true,
		},
		{
			key:      &AccountBalance,
			src:      nil,
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  false,
		},
		{
			key:      &AccountBalance,
			src:      (*uint256.Int)(nil),
			compared: new(uint256.Int).SetUint64(1),
			isEqual:  false,
		},
		{
			key:      &AccountBalance,
			src:      (*uint256.Int)(nil),
			compared: (*uint256.Int)(nil),
			isEqual:  true,
		},
		{
			key:      &AccountCodeHash,
			src:      []byte{1},
			compared: []byte{1},
			isEqual:  true,
		},
		{
			key:      &AccountCodeHash,
			src:      nil,
			compared: []byte{1},
			isEqual:  false,
		},
		{
			key:      &AccountCodeHash,
			src:      ([]byte)(nil),
			compared: []byte{1},
			isEqual:  false,
		},
		{
			key:      &AccountCodeHash,
			src:      ([]byte)(nil),
			compared: ([]byte)(nil),
			isEqual:  true,
		},
		{
			key:      &AccountSuicide,
			src:      struct{}{},
			compared: struct{}{},
			isEqual:  false,
		},
		{
			key:      &AccountSuicide,
			src:      nil,
			compared: struct{}{},
			isEqual:  false,
		},
		{
			key:      nil,
			src:      mockHash,
			compared: mockHash,
			isEqual:  true,
		},
		{
			key:      nil,
			src:      nil,
			compared: mockHash,
			isEqual:  false,
		},
	}

	for i, item := range tests {
		require.Equal(t, item.isEqual, isEqualRWVal(item.key, item.src, item.compared), i)
	}
}

func mockRWSet(index int, read []interface{}, write []interface{}) *RWSet {
	ver := StateVersion{
		TxIndex: index,
	}
	set := NewRWSet(ver)
	set.accReadSet[common.Address{}] = map[AccountState]RWItem{}
	set.accWriteSet[common.Address{}] = map[AccountState]RWItem{}
	set.slotReadSet[common.Address{}] = map[common.Hash]RWItem{}
	set.slotWriteSet[common.Address{}] = map[common.Hash]RWItem{}
	for _, k := range read {
		state, ok := k.(AccountState)
		if ok {
			set.accReadSet[common.Address{}][state] = RWItem{
				Ver: ver,
				Val: struct{}{},
			}
		} else {
			set.slotReadSet[common.Address{}][str2Slot(k.(string))] = RWItem{
				Ver: ver,
				Val: struct{}{},
			}
		}
	}
	for _, k := range write {
		state, ok := k.(AccountState)
		if ok {
			set.accWriteSet[common.Address{}][state] = RWItem{
				Ver: ver,
				Val: struct{}{},
			}
		} else {
			set.slotWriteSet[common.Address{}][str2Slot(k.(string))] = RWItem{
				Ver: ver,
				Val: struct{}{},
			}
		}
	}

	return set
}

func mockUintSlice(cnt int) []uint64 {
	ret := make([]uint64, cnt)
	for i := 0; i < cnt; i++ {
		ret[i] = rand.Uint64() % uint64(cnt)
	}
	return ret
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

	set.accReadSet[common.Address{}] = map[AccountState]RWItem{}
	set.slotReadSet[common.Address{}] = map[common.Hash]RWItem{}
	set.accWriteSet[common.Address{}] = map[AccountState]RWItem{}
	set.slotWriteSet[common.Address{}] = map[common.Hash]RWItem{}
	for i := 0; i < len(read); {
		state, ok := read[i].(AccountState)
		if ok {
			set.accReadSet[common.Address{}][state] = RWItem{
				Ver: StateVersion{
					TxIndex: index - 1,
				},
				Val: read[i+1],
			}
		} else {
			slot := str2Slot(read[i].(string))
			set.slotReadSet[common.Address{}][slot] = RWItem{
				Ver: StateVersion{
					TxIndex: index - 1,
				},
				Val: read[i+1],
			}
		}
		i += 2
	}
	for i := 0; i < len(write); {
		state, ok := write[i].(AccountState)
		if ok {
			set.accWriteSet[common.Address{}][state] = RWItem{
				Ver: ver,
				Val: write[i+1],
			}
		} else {
			slot := str2Slot(write[i].(string))
			set.slotWriteSet[common.Address{}][slot] = RWItem{
				Ver: ver,
				Val: write[i+1],
			}
		}
		i += 2
	}

	return set
}

func str2Slot(str string) common.Hash {
	return common.BytesToHash([]byte(str))
}

func mockRandomRWSet(count int) []*RWSet {
	var ret []*RWSet
	for i := 0; i < count; i++ {
		read := []interface{}{fmt.Sprintf("0x%d", i)}
		write := []interface{}{fmt.Sprintf("0x%d", i)}
		if i != 0 && rand.Bool() {
			depCnt := rand.Int()%i + 1
			last := 0
			for j := 0; j < depCnt; j++ {
				num, ok := randInRange(last, i)
				if !ok {
					break
				}
				read = append(read, fmt.Sprintf("0x%d", num))
				last = num
			}
		}
		// random read
		for j := 0; j < 20; j++ {
			read = append(read, fmt.Sprintf("rr-%d-%d", j, rand.Int()))
		}
		for j := 0; j < 5; j++ {
			read = append(read, fmt.Sprintf("rw-%d-%d", j, rand.Int()))
		}
		// random write
		s := mockRWSet(i, read, write)
		ret = append(ret, s)
	}
	return ret
}

func finaliseRWSets(t *testing.T, mv *MVStates, rwSets []*RWSet) {
	for i, rwSet := range rwSets {
		require.NoError(t, mv.FulfillRWSet(rwSet, nil))
		require.NoError(t, mv.Finalise(i))
	}
}

func randInRange(i, j int) (int, bool) {
	if i >= j {
		return 0, false
	}
	return rand.Int()%(j-i) + i, true
}
