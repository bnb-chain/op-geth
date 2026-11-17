package ethapi

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"testing"
)

type bundleAdmissionBackend struct {
	Backend
	headers int
	calls   int
}

func (b *bundleAdmissionBackend) CurrentHeader() *types.Header {
	b.headers++
	return &types.Header{Number: big.NewInt(1), Time: 1}
}
func (b *bundleAdmissionBackend) SendBundle(context.Context, *types.Bundle, *types.SendBundleArgs) error {
	b.calls++
	return nil
}
func (b *bundleAdmissionBackend) SimulateGaslessBundle(context.Context, *types.Bundle) (*types.SimulateGaslessBundleResp, error) {
	b.calls++
	return nil, nil
}
func TestBundleRPCRejectsBeforeDecoding(t *testing.T) {
	for _, gasless := range []bool{false, true} {
		for _, size := range []bool{false, true} {
			backend := new(bundleAdmissionBackend)
			api := NewPrivateTxBundleAPI(backend)
			txs := make([]hexutil.Bytes, 477)
			if size {
				txs = []hexutil.Bytes{make([]byte, 512*1024+1)}
			}
			var err error
			if gasless {
				_, err = api.SimulateGaslessBundle(context.Background(), types.SimulateGaslessBundleArgs{Txs: txs})
			} else {
				_, err = api.SendBundle(context.Background(), types.SendBundleArgs{Txs: txs})
			}
			if err == nil || backend.calls != 0 || backend.headers != 0 {
				t.Fatalf("gasless=%v,size=%v: err=%v, headers=%d, backend calls=%d", gasless, size, err, backend.headers, backend.calls)
			}
		}
	}
}
func TestBundleRPCRejectsUnknownRevertingHash(t *testing.T) {
	tx := types.NewTx(&types.LegacyTx{Gas: 21000})
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	backend := new(bundleAdmissionBackend)
	_, err = NewPrivateTxBundleAPI(backend).SendBundle(context.Background(), types.SendBundleArgs{Txs: []hexutil.Bytes{raw}, RevertingTxHashes: []common.Hash{{1}}})
	if err == nil || backend.calls != 0 {
		t.Fatalf("unknown reverting hash reached backend: %v", err)
	}
}
