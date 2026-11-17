package types

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	// MaxBundleAliveBlock is the max alive block for bundle.
	MaxBundleAliveBlock = 240
	// MaxBundleAliveTime is the max alive time for bundle.
	MaxBundleAliveTime = 60 // second
	// MaxBundleSize bounds decoding, hashing and state-independent validation.
	MaxBundleSize         = 512 * 1024
	MaxBundleTransactions = int(params.BundleGasLimit / params.TxGas)
)

// SendBundleArgs represents the arguments for a call.
type SendBundleArgs struct {
	Txs               []hexutil.Bytes `json:"txs"`
	MaxBlockNumber    uint64          `json:"maxBlockNumber"`
	MinTimestamp      *uint64         `json:"minTimestamp"`
	MaxTimestamp      *uint64         `json:"maxTimestamp"`
	RevertingTxHashes []common.Hash   `json:"revertingTxHashes"`
}

type Bundle struct {
	Txs               Transactions
	MaxBlockNumber    uint64
	MinTimestamp      uint64
	MaxTimestamp      uint64
	RevertingTxHashes []common.Hash

	Price *big.Int // for bundle compare and prune

	// caches
	hash atomic.Value
	size atomic.Value
}

// ValidateBasic checks state-independent limits before any simulation work.
func (bundle *Bundle) ValidateBasic() error {
	if bundle == nil || len(bundle.Txs) == 0 {
		return errors.New("bundle missing txs")
	}
	if len(bundle.Txs) > MaxBundleTransactions {
		return errors.New("too many bundle transactions")
	}
	if len(bundle.RevertingTxHashes) > len(bundle.Txs) {
		return errors.New("too many reverting transaction hashes")
	}
	txs := make(map[common.Hash]struct{}, len(bundle.Txs))
	var size uint64
	for _, tx := range bundle.Txs {
		if tx == nil {
			return errors.New("nil bundle transaction")
		}
		size += tx.Size()
		if size > MaxBundleSize {
			return errors.New("bundle exceeds size limit")
		}
		txs[tx.Hash()] = struct{}{}
	}
	seen := make(map[common.Hash]struct{}, len(bundle.RevertingTxHashes))
	for _, hash := range bundle.RevertingTxHashes {
		if _, ok := txs[hash]; !ok {
			return fmt.Errorf("reverting transaction %s is not in bundle", hash)
		}
		if _, ok := seen[hash]; ok {
			return errors.New("duplicate reverting transaction hash")
		}
		seen[hash] = struct{}{}
	}
	if bundle.Size() > MaxBundleSize {
		return errors.New("bundle exceeds size limit")
	}
	return nil
}

// SimulationHash identifies execution semantics, independent of admission
// windows, derived price and the order of the reverting-hash set.
func (bundle *Bundle) SimulationHash() common.Hash {
	txs := make([]common.Hash, len(bundle.Txs))
	for i, tx := range bundle.Txs {
		txs[i] = tx.Hash()
	}
	reverting := slices.Clone(bundle.RevertingTxHashes)
	slices.SortFunc(reverting, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) })
	return rlpHash([]interface{}{txs, reverting})
}

type SimulatedBundle struct {
	OriginalBundle *Bundle

	BundleGasFees  *big.Int
	BundleGasPrice *big.Int
	BundleGasUsed  uint64
}

func (bundle *Bundle) Size() uint64 {
	if size := bundle.size.Load(); size != nil {
		return size.(uint64)
	}
	c := writeCounter(0)
	rlp.Encode(&c, bundle)

	size := uint64(c)
	bundle.size.Store(size)
	return size
}

// Hash returns the bundle hash.
func (bundle *Bundle) Hash() common.Hash {
	if hash := bundle.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}

	h := rlpHash(bundle)
	bundle.hash.Store(h)
	return h
}
