// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package miner

import (
	"crypto/sha256"
	"encoding/binary"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

// BuildPayloadArgs contains the provided parameters for building payload.
// Check engine-api specification for more details.
// https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#payloadattributesv3
type BuildPayloadArgs struct {
	Parent       common.Hash       // The parent block to build payload on top
	Timestamp    uint64            // The provided timestamp of generated payload
	FeeRecipient common.Address    // The provided recipient address for collecting transaction fee
	Random       common.Hash       // The provided randomness value
	Withdrawals  types.Withdrawals // The provided withdrawals
	BeaconRoot   *common.Hash      // The provided beaconRoot (Cancun)

	NoTxPool     bool                 // Optimism addition: option to disable tx pool contents from being included
	Transactions []*types.Transaction // Optimism addition: txs forced into the block via engine API
	GasLimit     *uint64              // Optimism addition: override gas limit of the block to build
}

// Id computes an 8-byte identifier by hashing the components of the payload arguments.
func (args *BuildPayloadArgs) Id() engine.PayloadID {
	// Hash
	hasher := sha256.New()
	hasher.Write(args.Parent[:])
	binary.Write(hasher, binary.BigEndian, args.Timestamp)
	hasher.Write(args.Random[:])
	hasher.Write(args.FeeRecipient[:])
	rlp.Encode(hasher, args.Withdrawals)
	if args.BeaconRoot != nil {
		hasher.Write(args.BeaconRoot[:])
	}

	if args.NoTxPool || len(args.Transactions) > 0 { // extend if extra payload attributes are used
		binary.Write(hasher, binary.BigEndian, args.NoTxPool)
		binary.Write(hasher, binary.BigEndian, uint64(len(args.Transactions)))
		for _, tx := range args.Transactions {
			h := tx.Hash()
			hasher.Write(h[:])
		}
	}
	if args.GasLimit != nil {
		binary.Write(hasher, binary.BigEndian, *args.GasLimit)
	}

	var out engine.PayloadID
	copy(out[:], hasher.Sum(nil)[:8])
	return out
}

// Payload wraps the built payload(block waiting for sealing). According to the
// engine-api specification, EL should build the initial version of the payload
// which has an empty transaction set and then keep update it in order to maximize
// the revenue. Therefore, the empty-block here is always available and full-block
// will be set/updated afterwards.
type Payload struct {
	id       engine.PayloadID
	empty    *types.Block
	full     *types.Block
	sidecars []*types.BlobTxSidecar
	fullFees *big.Int
	stop     chan struct{}
	lock     sync.Mutex
	cond     *sync.Cond
}

// newPayload initializes the payload object.
func newPayload(empty *types.Block, id engine.PayloadID) *Payload {
	payload := &Payload{
		id:    id,
		empty: empty,
		stop:  make(chan struct{}),
	}
	log.Info("Starting work on payload", "id", payload.id)
	payload.cond = sync.NewCond(&payload.lock)
	return payload
}

// update updates the full-block with latest built version.
func (payload *Payload) update(r *newPayloadResult, elapsed time.Duration, postFuncs ...func()) {
	payload.lock.Lock()
	defer payload.lock.Unlock()

	select {
	case <-payload.stop:
		return // reject stale update
	default:
	}
	// Ensure the newly provided full block has a higher transaction fee.
	// In post-merge stage, there is no uncle reward anymore and transaction
	// fee(apart from the mev revenue) is the only indicator for comparison.
	if payload.full == nil || r.fees.Cmp(payload.fullFees) > 0 {
		payload.full = r.block
		payload.fullFees = r.fees
		payload.sidecars = r.sidecars

		feesInEther := new(big.Float).Quo(new(big.Float).SetInt(r.fees), big.NewFloat(params.Ether))
		log.Info("Updated payload",
			"id", payload.id,
			"number", r.block.NumberU64(),
			"hash", r.block.Hash(),
			"txs", len(r.block.Transactions()),
			"withdrawals", len(r.block.Withdrawals()),
			"gas", r.block.GasUsed(),
			"fees", feesInEther,
			"root", r.block.Root(),
			"elapsed", common.PrettyDuration(elapsed),
		)

		for _, postFunc := range postFuncs {
			if postFunc != nil {
				postFunc()
			}
		}
	}
	payload.cond.Broadcast() // fire signal for notifying full block
}

// Resolve returns the latest built payload and also terminates the background
// thread for updating payload. It's safe to be called multiple times.
func (payload *Payload) Resolve() *engine.ExecutionPayloadEnvelope {
	payload.lock.Lock()
	defer payload.lock.Unlock()

	select {
	case <-payload.stop:
	default:
		close(payload.stop)
	}
	if payload.full != nil {
		return engine.BlockToExecutableData(payload.full, payload.fullFees, payload.sidecars)
	}
	return engine.BlockToExecutableData(payload.empty, big.NewInt(0), nil)
}

// ResolveEmpty is basically identical to Resolve, but it expects empty block only.
// It's only used in tests.
func (payload *Payload) ResolveEmpty() *engine.ExecutionPayloadEnvelope {
	payload.lock.Lock()
	defer payload.lock.Unlock()

	return engine.BlockToExecutableData(payload.empty, big.NewInt(0), nil)
}

// ResolveFull is basically identical to Resolve, but it expects full block only.
// Don't call Resolve until ResolveFull returns, otherwise it might block forever.
func (payload *Payload) ResolveFull() *engine.ExecutionPayloadEnvelope {
	payload.lock.Lock()
	defer payload.lock.Unlock()

	if payload.full == nil {
		select {
		case <-payload.stop:
			return nil
		default:
		}
		// Wait the full payload construction. Note it might block
		// forever if Resolve is called in the meantime which
		// terminates the background construction process.
		payload.cond.Wait()
	}
	// Terminate the background payload construction
	select {
	case <-payload.stop:
	default:
		close(payload.stop)
	}
	return engine.BlockToExecutableData(payload.full, payload.fullFees, payload.sidecars)
}

// buildPayload builds the payload according to the provided parameters.
func (w *worker) buildPayload(args *BuildPayloadArgs) (*Payload, error) {
	// Build the initial version with no transaction included. It should be fast
	// enough to run. The empty payload can at least make sure there is something
	// to deliver for not missing slot.
	// In OP-Stack, the "empty" block is constructed from provided txs only, i.e. no tx-pool usage.
	emptyParams := &generateParams{
		timestamp:   args.Timestamp,
		forceTime:   true,
		parentHash:  args.Parent,
		coinbase:    args.FeeRecipient,
		random:      args.Random,
		withdrawals: args.Withdrawals,
		beaconRoot:  args.BeaconRoot,
		noTxs:       true,
		txs:         args.Transactions,
		gasLimit:    args.GasLimit,
	}
	start := time.Now()
	empty := w.getSealingBlock(emptyParams)
	if empty.err != nil {
		log.Error("Built initial payload error", "id", args.Id(), "error", empty.err)
		return nil, empty.err
	}
	log.Debug("Built initial payload", "id", args.Id(), "number", empty.block.NumberU64(), "hash", empty.block.Hash(), "elapsed", common.PrettyDuration(time.Since(start)))

	// Construct a payload object for return.
	payload := newPayload(empty.block, args.Id())
	if args.NoTxPool { // don't start the background payload updating job if there is no tx pool to pull from
		// make sure to make it appear as full, otherwise it will wait indefinitely for payload building to complete.
		payload.full = empty.block
		payload.fullFees = empty.fees
		return payload, nil
	}

	// Spin up a routine for updating the payload in background. This strategy
	// can maximum the revenue for including transactions with highest fee.
	go func() {
		// Setup the timer for re-building the payload. The initial clock is kept
		// for triggering process immediately.
		timer := time.NewTimer(0)
		defer timer.Stop()

		// Setup the timer for terminating the process if SECONDS_PER_SLOT (12s in
		// the Mainnet configuration) have passed since the point in time identified
		// by the timestamp parameter.
		endTimer := time.NewTimer(time.Second * 12)

		fullParams := &generateParams{
			timestamp:   args.Timestamp,
			forceTime:   true,
			parentHash:  args.Parent,
			coinbase:    args.FeeRecipient,
			random:      args.Random,
			withdrawals: args.Withdrawals,
			beaconRoot:  args.BeaconRoot,
			noTxs:       false,
			txs:         args.Transactions,
			gasLimit:    args.GasLimit,
		}

		for {
			select {
			case <-timer.C:
				start := time.Now()
				r := w.getSealingBlock(fullParams)
				if r.err != nil {
					log.Error("Failed to build updated payload", "id", payload.id, "err", r.err)
					return
				}
				log.Debug("Built updated payload", "id", payload.id, "number", r.block.NumberU64(), "hash", r.block.Hash(), "elapsed", common.PrettyDuration(time.Since(start)))
				payload.update(r, time.Since(start), func() {
					w.cacheMiningBlock(r.block, r.env)
				})
				timer.Reset(w.recommit)
			case <-payload.stop:
				log.Info("Stopping work on payload", "id", payload.id, "reason", "delivery")
				return
			case <-endTimer.C:
				log.Info("Stopping work on payload", "id", payload.id, "reason", "timeout")
				return
			}
		}
	}()
	return payload, nil
}

func (w *worker) cacheMiningBlock(block *types.Block, env *environment) {
	var (
		start    = time.Now()
		receipts = make([]*types.Receipt, len(env.receipts))
		logs     []*types.Log
		hash     = block.Hash()
	)
	for i, taskReceipt := range env.receipts {
		receipt := new(types.Receipt)
		receipts[i] = receipt
		*receipt = *taskReceipt

		// add block location fields
		receipt.BlockHash = hash
		receipt.BlockNumber = block.Number()
		receipt.TransactionIndex = uint(i)

		// Update the block hash in all logs since it is now available and not when the
		// receipt/log of individual transactions were created.
		receipt.Logs = make([]*types.Log, len(taskReceipt.Logs))
		for i, taskLog := range taskReceipt.Logs {
			log := new(types.Log)
			receipt.Logs[i] = log
			*log = *taskLog
			log.BlockHash = hash
		}
		logs = append(logs, receipt.Logs...)
	}

	w.chain.CacheMiningReceipts(hash, receipts)
	w.chain.CacheMiningTxLogs(hash, logs)
	w.chain.CacheMiningState(hash, env.state)

	log.Info("Successfully cached sealed new block", "number", block.Number(), "root", block.Root(), "hash", hash,
		"elapsed", common.PrettyDuration(time.Since(start)))
}
