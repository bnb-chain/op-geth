package ethapi

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

const InvalidBundleParamError = -38000

// PrivateTxBundleAPI offers an API for accepting bundled transactions
type PrivateTxBundleAPI struct {
	b Backend
}

// NewPrivateTxBundleAPI creates a new Tx Bundle API instance.
func NewPrivateTxBundleAPI(b Backend) *PrivateTxBundleAPI {
	return &PrivateTxBundleAPI{b}
}

func (s *PrivateTxBundleAPI) BundlePrice(ctx context.Context) *big.Int {
	return s.b.BundlePrice()
}

// SimulateGaslessBundle simulates the execution of a list of transactions with order
func (s *PrivateTxBundleAPI) SimulateGaslessBundle(ctx context.Context, args types.SimulateGaslessBundleArgs) (*types.SimulateGaslessBundleResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateEncodedBundle(args.Txs); err != nil {
		return nil, newBundleError(err)
	}

	var txs types.Transactions

	for _, encodedTx := range args.Txs {
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(encodedTx); err != nil {
			log.Error("failed to unmarshal gasless tx", "err", err)
			continue
		}
		switch tx.Type() {
		case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType:
		default:
			log.Error("transaction type not supported", "txType", tx.Type())
			continue
		}
		txs = append(txs, tx)
	}

	bundle := &types.Bundle{
		Txs: txs,
	}

	if err := bundle.ValidateBasic(); err != nil {
		return nil, newBundleError(err)
	}
	return s.b.SimulateGaslessBundle(ctx, bundle)
}

// SendBundle will add the signed transaction to the transaction pool.
// The sender is responsible for signing the transaction and using the correct nonce and ensuring validity
func (s *PrivateTxBundleAPI) SendBundle(ctx context.Context, args types.SendBundleArgs) (common.Hash, error) {
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	if err := validateEncodedBundle(args.Txs); err != nil {
		return common.Hash{}, newBundleError(err)
	}
	if len(args.RevertingTxHashes) > len(args.Txs) {
		return common.Hash{}, newBundleError(errors.New("too many reverting transaction hashes"))
	}

	currentHeader := s.b.CurrentHeader()

	if args.MaxBlockNumber == 0 && (args.MaxTimestamp == nil || *args.MaxTimestamp == 0) {
		maxTimeStamp := currentHeader.Time + types.MaxBundleAliveTime
		args.MaxTimestamp = &maxTimeStamp
	}

	if args.MaxBlockNumber != 0 && args.MaxBlockNumber > currentHeader.Number.Uint64()+types.MaxBundleAliveBlock {
		return common.Hash{}, newBundleError(errors.New(fmt.Sprintf("the maxBlockNumber should not be lager than currentBlockNum + %d", types.MaxBundleAliveBlock)))
	}

	if args.MaxTimestamp != nil && args.MinTimestamp != nil && *args.MaxTimestamp != 0 && *args.MinTimestamp != 0 {
		if *args.MaxTimestamp <= *args.MinTimestamp {
			return common.Hash{}, newBundleError(errors.New("the maxTimestamp should not be less than minTimestamp"))
		}
	}

	if args.MaxTimestamp != nil && *args.MaxTimestamp != 0 && *args.MaxTimestamp < currentHeader.Time {
		return common.Hash{}, newBundleError(errors.New("the maxTimestamp should not be less than currentBlockTimestamp"))
	}

	if (args.MaxTimestamp != nil && *args.MaxTimestamp > currentHeader.Time+types.MaxBundleAliveTime) ||
		(args.MinTimestamp != nil && *args.MinTimestamp > currentHeader.Time+types.MaxBundleAliveTime) {
		return common.Hash{}, newBundleError(errors.New(fmt.Sprintf("the minTimestamp/maxTimestamp should not be later than currentBlockTimestamp + %d seconds", types.MaxBundleAliveTime)))
	}

	var txs types.Transactions

	for _, encodedTx := range args.Txs {
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(encodedTx); err != nil {
			return common.Hash{}, err
		}
		txs = append(txs, tx)
	}

	var minTimestamp, maxTimestamp uint64

	if args.MinTimestamp != nil {
		minTimestamp = *args.MinTimestamp
	}

	if args.MaxTimestamp != nil {
		maxTimestamp = *args.MaxTimestamp
	}

	bundle := &types.Bundle{
		Txs:               txs,
		MaxBlockNumber:    args.MaxBlockNumber,
		MinTimestamp:      minTimestamp,
		MaxTimestamp:      maxTimestamp,
		RevertingTxHashes: args.RevertingTxHashes,
	}

	// If the maxBlockNumber and maxTimestamp are not set, set max ddl of bundle as types.MaxBundleAliveBlock
	if bundle.MaxBlockNumber == 0 && bundle.MaxTimestamp == 0 {
		bundle.MaxBlockNumber = currentHeader.Number.Uint64() + types.MaxBundleAliveBlock
	}

	if err := bundle.ValidateBasic(); err != nil {
		return common.Hash{}, newBundleError(err)
	}
	hash := bundle.Hash()
	log.Info("Received bundle via RPC", "hash", hash, "txCount", len(txs), "maxBlockNumber", bundle.MaxBlockNumber, "minTimestamp", minTimestamp, "maxTimestamp", maxTimestamp)

	err := s.b.SendBundle(ctx, bundle, &args)
	if err != nil {
		log.Error("Failed to add bundle", "hash", hash, "err", err)
		return common.Hash{}, err
	}

	log.Info("Bundle accepted into pool", "hash", hash)
	return hash, nil
}

func newBundleError(err error) *bundleError {
	return &bundleError{
		error: err,
	}
}

// bundleError is an API error that encompasses an invalid bundle with JSON error
// code and a binary data blob.
type bundleError struct {
	error
}

// ErrorCode returns the JSON error code for an invalid bundle.
// See: https://github.com/ethereum/wiki/wiki/JSON-RPC-Error-Codes-Improvement-Proposal
func (e *bundleError) ErrorCode() int {
	return InvalidBundleParamError
}

// validateEncodedBundle caps work before allocating and decoding transactions.
func validateEncodedBundle(txs []hexutil.Bytes) error {
	if len(txs) == 0 {
		return errors.New("bundle missing txs")
	}
	if len(txs) > types.MaxBundleTransactions {
		return errors.New("too many bundle transactions")
	}
	var size uint64
	for _, tx := range txs {
		size += uint64(len(tx))
		if size > types.MaxBundleSize {
			return errors.New("bundle exceeds size limit")
		}
	}
	return nil
}
