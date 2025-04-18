// Copyright 2014 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
)

var (
	commitDepositTxsTimer = metrics.NewRegisteredTimer("miner/commit/deposit/txs", nil)
	packFromTxpoolTimer   = metrics.NewRegisteredTimer("miner/pack/txpool/txs", nil)
	commitTxpoolTxsTimer  = metrics.NewRegisteredTimer("miner/commit/txpool/txs", nil)
	assembleBlockTimer    = metrics.NewRegisteredTimer("miner/assemble/block", nil)
	buildBlockTimer       = metrics.NewRegisteredTimer("miner/build/block", nil)

	accountReadTimer   = metrics.NewRegisteredTimer("miner/account/reads", nil)
	accountHashTimer   = metrics.NewRegisteredTimer("miner/account/hashes", nil)
	accountUpdateTimer = metrics.NewRegisteredTimer("miner/account/updates", nil)

	storageReadTimer   = metrics.NewRegisteredTimer("miner/storage/reads", nil)
	storageHashTimer   = metrics.NewRegisteredTimer("miner/storage/hashes", nil)
	storageUpdateTimer = metrics.NewRegisteredTimer("miner/storage/updates", nil)

	innerExecutionTimer = metrics.NewRegisteredTimer("miner/inner/execution", nil)

	snapshotAccountReadTimer = metrics.NewRegisteredTimer("miner/snapshot/account/reads", nil)
	snapshotStorageReadTimer = metrics.NewRegisteredTimer("miner/snapshot/storage/reads", nil)

	waitPayloadTimer   = metrics.NewRegisteredTimer("miner/wait/payload", nil)
	txDAGGenerateTimer = metrics.NewRegisteredTimer("miner/txdag/gen", nil)

	isBuildBlockInterruptCounter = metrics.NewRegisteredCounter("miner/build/interrupt", nil)
)

var defaultCoinBaseAddress = common.HexToAddress("0x4200000000000000000000000000000000000011")

type MevConfig struct {
	MevEnabled             bool     // Whether to enable Mev or not
	MevReceivers           []string // The list of Mev bundle receivers
	MevBundleGasPriceFloor int64    // The minimal bundle gas Price
}

var DefaultMevConfig = MevConfig{
	MevEnabled:             false,
	MevReceivers:           nil,
	MevBundleGasPriceFloor: 1,
}

// Backend wraps all methods required for mining. Only full node is capable
// to offer all the functions here.
type Backend interface {
	BlockChain() *core.BlockChain
	TxPool() *txpool.TxPool
}

type BackendWithHistoricalState interface {
	StateAtBlock(ctx context.Context, block *types.Block, reexec uint64, base *state.StateDB, readOnly bool, preferDisk bool) (*state.StateDB, tracers.StateReleaseFunc, error)
}

// Config is the configuration parameters of mining.
type Config struct {
	Etherbase common.Address `toml:",omitempty"` // Public address for block mining rewards
	ExtraData hexutil.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	GasFloor  uint64         // Target gas floor for mined blocks.
	GasCeil   uint64         // Target gas ceiling for mined blocks.
	GasPrice  *big.Int       // Minimum gas price for mining a transaction
	Recommit  time.Duration  // The time interval for miner to re-create mining work.

	NewPayloadTimeout time.Duration // The maximum time allowance for creating a new payload

	RollupComputePendingBlock bool   // Compute the pending block from tx-pool, instead of copying the latest-block
	EffectiveGasCeil          uint64 // if non-zero, a gas ceiling to apply independent of the header's gaslimit value

	Mev MevConfig // Mev configuration

	ParallelTxDAGSenderPriv *ecdsa.PrivateKey // The private key for the parallel tx DAG sender
}

// DefaultConfig contains default settings for miner.
var DefaultConfig = Config{
	GasCeil:  30000000,
	GasPrice: big.NewInt(params.Wei),

	// The default recommit time is chosen as two seconds since
	// consensus-layer usually will wait a half slot of time(6s)
	// for payload generation. It should be enough for Geth to
	// run 3 rounds.
	Recommit:          2 * time.Second,
	NewPayloadTimeout: 2 * time.Second,

	Mev: DefaultMevConfig,
}

// Miner creates blocks and searches for proof-of-work values.
type Miner struct {
	mux     *event.TypeMux
	eth     Backend
	engine  consensus.Engine
	exitCh  chan struct{}
	startCh chan struct{}
	stopCh  chan struct{}
	worker  *worker

	wg sync.WaitGroup
}

func New(eth Backend, config *Config, chainConfig *params.ChainConfig, mux *event.TypeMux, engine consensus.Engine, isLocalBlock func(header *types.Header) bool) *Miner {
	miner := &Miner{
		mux:     mux,
		eth:     eth,
		engine:  engine,
		exitCh:  make(chan struct{}),
		startCh: make(chan struct{}),
		stopCh:  make(chan struct{}),
		worker:  newWorker(config, chainConfig, engine, eth, mux, isLocalBlock, true),
	}
	miner.wg.Add(1)
	go miner.update()
	return miner
}

// update keeps track of the downloader events. Please be aware that this is a one shot type of update loop.
// It's entered once and as soon as `Done` or `Failed` has been broadcasted the events are unregistered and
// the loop is exited. This to prevent a major security vuln where external parties can DOS you with blocks
// and halt your mining operation for as long as the DOS continues.
func (miner *Miner) update() {
	defer miner.wg.Done()

	events := miner.mux.Subscribe(downloader.StartEvent{}, downloader.DoneEvent{}, downloader.FailedEvent{})
	defer func() {
		if !events.Closed() {
			events.Unsubscribe()
		}
	}()

	shouldStart := false
	canStart := true
	dlEventCh := events.Chan()
	for {
		select {
		case ev := <-dlEventCh:
			if ev == nil {
				// Unsubscription done, stop listening
				dlEventCh = nil
				continue
			}
			switch ev.Data.(type) {
			case downloader.StartEvent:
				wasMining := miner.Mining()
				miner.worker.stop()
				canStart = false
				if wasMining {
					// Resume mining after sync was finished
					shouldStart = true
					log.Info("Mining aborted due to sync")
				}
				miner.worker.syncing.Store(true)

			case downloader.FailedEvent:
				canStart = true
				if shouldStart {
					miner.worker.start()
				}
				miner.worker.syncing.Store(false)

			case downloader.DoneEvent:
				canStart = true
				if shouldStart {
					miner.worker.start()
				}
				miner.worker.syncing.Store(false)

				// Stop reacting to downloader events
				events.Unsubscribe()
			}
		case <-miner.startCh:
			if canStart {
				miner.worker.start()
			}
			shouldStart = true
		case <-miner.stopCh:
			shouldStart = false
			miner.worker.stop()
		case <-miner.exitCh:
			miner.worker.close()
			return
		}
	}
}

func (miner *Miner) Start() {
	miner.startCh <- struct{}{}
}

func (miner *Miner) Stop() {
	miner.stopCh <- struct{}{}
}

func (miner *Miner) Close() {
	close(miner.exitCh)
	miner.wg.Wait()
}

func (miner *Miner) Mining() bool {
	return miner.worker.isRunning()
}

func (miner *Miner) Hashrate() uint64 {
	if pow, ok := miner.engine.(consensus.PoW); ok {
		return uint64(pow.Hashrate())
	}
	return 0
}

func (miner *Miner) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	miner.worker.setExtra(extra)
	return nil
}

func (miner *Miner) SetGasTip(tip *big.Int) error {
	miner.worker.setGasTip(tip)
	return nil
}

// SetRecommitInterval sets the interval for sealing work resubmitting.
func (miner *Miner) SetRecommitInterval(interval time.Duration) {
	miner.worker.setRecommitInterval(interval)
}

// Pending returns the currently pending block and associated state. The returned
// values can be nil in case the pending block is not initialized
func (miner *Miner) Pending() (*types.Block, *state.StateDB) {
	return miner.worker.pending()
}

// PendingBlock returns the currently pending block. The returned block can be
// nil in case the pending block is not initialized.
//
// Note, to access both the pending block and the pending state
// simultaneously, please use Pending(), as the pending state can
// change between multiple method calls
func (miner *Miner) PendingBlock() *types.Block {
	return miner.worker.pendingBlock()
}

// PendingBlockAndReceipts returns the currently pending block and corresponding receipts.
// The returned values can be nil in case the pending block is not initialized.
func (miner *Miner) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return miner.worker.pendingBlockAndReceipts()
}

func (miner *Miner) SetEtherbase(addr common.Address) {
	miner.worker.setEtherbase(addr)
}

// SetGasCeil sets the gaslimit to strive for when mining blocks post 1559.
// For pre-1559 blocks, it sets the ceiling.
func (miner *Miner) SetGasCeil(ceil uint64) {
	miner.worker.setGasCeil(ceil)
}

// SubscribePendingLogs starts delivering logs from pending transactions
// to the given channel.
func (miner *Miner) SubscribePendingLogs(ch chan<- []*types.Log) event.Subscription {
	return miner.worker.pendingLogsFeed.Subscribe(ch)
}

// BuildPayload builds the payload according to the provided parameters.
func (miner *Miner) BuildPayload(args *BuildPayloadArgs) (*Payload, error) {
	return miner.worker.buildPayload(args)
}

func (miner *Miner) SimulateBundle(bundle *types.Bundle) (*big.Int, error) {

	env, err := miner.prepareSimulationEnv()
	if err != nil {
		return nil, err
	}

	s, err := miner.worker.simulateBundles(env, []*types.Bundle{bundle})
	if err != nil {
		return nil, err
	}

	if len(s) == 0 {
		return nil, errors.New("no valid sim result")
	}

	return s[0].BundleGasPrice, nil
}

func (miner *Miner) SimulateGaslessBundle(bundle *types.Bundle) (*types.SimulateGaslessBundleResp, error) {

	env, err := miner.prepareSimulationEnv()
	if err != nil {
		return nil, err
	}

	resp, err := miner.worker.simulateGaslessBundle(env, bundle)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (miner *Miner) prepareSimulationEnv() (*environment, error) {
	parent := miner.eth.BlockChain().CurrentBlock()
	// fork check
	timestamp := parent.NextMilliTimestamp()

	var mixDigest common.Hash
	milliPartBytes := uint256.NewInt(timestamp % 1000).Bytes32()
	mixDigest[0] = milliPartBytes[30]
	mixDigest[1] = milliPartBytes[31]
	mixDigest[2] = parent.MixDigest[2]

	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     new(big.Int).Add(parent.Number, common.Big1),
		GasLimit:   core.CalcGasLimit(parent.GasLimit, miner.worker.config.GasCeil),
		Time:       timestamp / 1000,
		MixDigest:  mixDigest,
		Coinbase:   defaultCoinBaseAddress,
	}

	// Set baseFee and GasLimit if we are on an EIP-1559 chain
	if miner.worker.chainConfig.IsLondon(header.Number) {
		header.BaseFee = eip1559.CalcBaseFee(miner.worker.chainConfig, parent, header.Time)
	}

	if miner.worker.chainConfig.Optimism != nil && miner.worker.config.GasCeil != 0 {
		// configure the gas limit of pending blocks with the miner gas limit config when using optimism
		header.GasLimit = miner.worker.config.GasCeil
	}

	// Apply EIP-4844, EIP-4788.
	if miner.worker.chainConfig.IsCancun(header.Number, header.Time) {
		var excessBlobGas uint64
		if miner.worker.chainConfig.IsCancun(parent.Number, parent.Time) {
			excessBlobGas = eip4844.CalcExcessBlobGas(*parent.ExcessBlobGas, *parent.BlobGasUsed)
		} else {
			// For the first post-fork block, both parent.data_gas_used and parent.excess_data_gas are evaluated as 0
			excessBlobGas = eip4844.CalcExcessBlobGas(0, 0)
		}
		header.BlobGasUsed = new(uint64)
		header.ExcessBlobGas = &excessBlobGas
	}

	if err := miner.worker.engine.Prepare(miner.eth.BlockChain(), header); err != nil {
		log.Error("Failed to prepare header for simulateBundle", "err", err)
		return nil, err
	}

	state, err := miner.eth.BlockChain().StateAt(parent.Root)
	if err != nil {
		return nil, err
	}

	env := &environment{
		header:  header,
		state:   state.Copy(),
		signer:  types.MakeSigner(miner.worker.chainConfig, header.Number, header.Time),
		gasPool: prepareGasPool(),
	}
	return env, nil
}
