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
	mockRWSetSize = 10000
)

func TestMVStates_SimpleResolveTxDAG(t *testing.T) {
	ms := NewMVStates(10, nil).EnableAsyncGen()
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

	dag, err := ms.ResolveTxDAG(10)
	require.NoError(t, err)
	require.Equal(t, mockSimpleDAG(), dag)
	t.Log(dag)
}

func TestMVStates_ResolveTxDAG_Compare(t *testing.T) {
	txCnt := 3000
	rwSets := mockRandomRWSet(txCnt)
	ms1 := NewMVStates(txCnt, nil).EnableAsyncGen()
	ms2 := NewMVStates(txCnt, nil).EnableAsyncGen()
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
		require.NoError(t, ms2.FinaliseWithRWSet(rwSet))
	}

	d1 := resolveTxDAGInMVStates(ms1, txCnt)
	d2 := resolveDepsMapCacheByWritesInMVStates(ms2)
	require.Equal(t, d1.(*PlainTxDAG).String(), d2.(*PlainTxDAG).String())
}

func TestMVStates_TxDAG_Compression(t *testing.T) {
	txCnt := 10000
	rwSets := mockRandomRWSet(txCnt)
	ms1 := NewMVStates(txCnt, nil).EnableAsyncGen()
	for _, rwSet := range rwSets {
		ms1.FinaliseWithRWSet(rwSet)
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
	ms1 := NewMVStates(mockRWSetSize, nil).EnableAsyncGen()
	for i, rwSet := range rwSets {
		ms1.rwSets[i] = rwSet
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolveTxDAGInMVStates(ms1, mockRWSetSize)
	}
}

func BenchmarkResolveTxDAGByWritesInMVStates(b *testing.B) {
	rwSets := mockRandomRWSet(mockRWSetSize)
	ms1 := NewMVStates(mockRWSetSize, nil).EnableAsyncGen()
	for _, rwSet := range rwSets {
		ms1.FinaliseWithRWSet(rwSet)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolveDepsMapCacheByWritesInMVStates(ms1)
	}
}

func BenchmarkMVStates_Finalise(b *testing.B) {
	rwSets := mockRandomRWSet(mockRWSetSize)
	ms1 := NewMVStates(mockRWSetSize, nil).EnableAsyncGen()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, rwSet := range rwSets {
			ms1.FinaliseWithRWSet(rwSet)
		}
	}
}

func resolveTxDAGInMVStates(s *MVStates, txCnt int) TxDAG {
	txDAG := NewPlainTxDAG(txCnt)
	for i := 0; i < txCnt; i++ {
		s.resolveDepsCache(i, s.rwSets[i])
		txDAG.TxDeps[i] = s.txDepCache[i]
	}
	return txDAG
}

func resolveDepsMapCacheByWritesInMVStates(s *MVStates) TxDAG {
	txCnt := s.nextFinaliseIndex
	txDAG := NewPlainTxDAG(txCnt)
	for i := 0; i < txCnt; i++ {
		s.resolveDepsMapCacheByWrites(i, s.rwSets[i])
		txDAG.TxDeps[i] = s.txDepCache[i]
	}
	return txDAG
}

func TestMVStates_SystemTxResolveTxDAG(t *testing.T) {
	ms := NewMVStates(12, nil).EnableAsyncGen()
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

	dag, err := ms.ResolveTxDAG(12)
	require.NoError(t, err)
	require.Equal(t, mockSystemTxDAG(), dag)
	t.Log(dag)
}

func TestMVStates_SystemTxWithLargeDepsResolveTxDAG(t *testing.T) {
	ms := NewMVStates(12, nil).EnableAsyncGen()
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
	dag, err := ms.ResolveTxDAG(12)
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

func TestTxRecorder_Basic(t *testing.T) {
	sets := []*RWSet{
		mockRWSet(0, []interface{}{AccountSelf, AccountBalance, "0x00"},
			[]interface{}{AccountBalance, AccountCodeHash, "0x00"}),
		mockRWSet(1, []interface{}{AccountSelf, AccountBalance, "0x01"},
			[]interface{}{AccountBalance, AccountCodeHash, "0x01"}),
		mockRWSet(2, []interface{}{AccountSelf, AccountBalance, "0x01", "0x01"},
			[]interface{}{AccountBalance, AccountCodeHash, "0x01"}),
	}
	ms := NewMVStates(0, nil).EnableAsyncGen()
	for _, item := range sets {
		ms.RecordNewTx(item.index)
		for addr, sub := range item.accReadSet {
			for state := range sub {
				ms.RecordAccountRead(addr, state)
			}
		}
		for addr, sub := range item.slotReadSet {
			for slot := range sub {
				ms.RecordStorageRead(addr, slot)
			}
		}
		for addr, sub := range item.accWriteSet {
			for state := range sub {
				ms.RecordAccountWrite(addr, state)
			}
		}
		for addr, sub := range item.slotWriteSet {
			for slot := range sub {
				ms.RecordStorageWrite(addr, slot)
			}
		}
	}
	dag, err := ms.ResolveTxDAG(3)
	require.NoError(t, err)
	t.Log(dag)
}

func mockRWSet(index int, read []interface{}, write []interface{}) *RWSet {
	set := NewRWSet(index)
	set.accReadSet[common.Address{}] = map[AccountState]struct{}{}
	set.accWriteSet[common.Address{}] = map[AccountState]struct{}{}
	set.slotReadSet[common.Address{}] = map[common.Hash]struct{}{}
	set.slotWriteSet[common.Address{}] = map[common.Hash]struct{}{}
	for _, k := range read {
		state, ok := k.(AccountState)
		if ok {
			set.accReadSet[common.Address{}][state] = struct{}{}
		} else {
			set.slotReadSet[common.Address{}][str2Slot(k.(string))] = struct{}{}
		}
	}
	for _, k := range write {
		state, ok := k.(AccountState)
		if ok {
			set.accWriteSet[common.Address{}][state] = struct{}{}
		} else {
			set.slotWriteSet[common.Address{}][str2Slot(k.(string))] = struct{}{}
		}
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
	for _, rwSet := range rwSets {
		require.NoError(t, mv.FinaliseWithRWSet(rwSet))
	}
}

func randInRange(i, j int) (int, bool) {
	if i >= j {
		return 0, false
	}
	return rand.Int()%(j-i) + i, true
}
