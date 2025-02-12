// Copyright 2020 The go-ethereum Authors
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

package eth

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	// This is the target size for the packs of transactions or announcements. A
	// pack can get larger than this if a single transactions exceeds this size.
	maxTxPacketSize = 100 * 1024
)

var (
	txAnnounceAbandonMeter  = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/abandon", nil)
	txBroadcastAbandonMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/abandon", nil)
	txP2PAnnQueueGauge      = metrics.NewRegisteredGauge("eth/fetcher/transaction/p2p/ann/queue", nil)
	txP2PBroadQueueGauge    = metrics.NewRegisteredGauge("eth/fetcher/transaction/p2p/broad/queue", nil)
)

// safeGetPeerIP
var safeGetPeerIP = func(p *Peer) string {
	if p.Node() != nil && p.Node().IP() != nil {
		return p.Node().IP().String()
	}
	return "UNKNOWN"
}

// blockPropagation is a block propagation event, waiting for its turn in the
// broadcast queue.
type blockPropagation struct {
	block *types.Block
	td    *big.Int
}

// broadcastBlocks is a write loop that multiplexes blocks and block announcements
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) broadcastBlocks() {
	for {
		select {
		case prop := <-p.queuedBlocks:
			if err := p.SendNewBlock(prop.block, prop.td); err != nil {
				return
			}
			p.Log().Trace("Propagated block", "number", prop.block.Number(), "hash", prop.block.Hash(), "td", prop.td)

		case block := <-p.queuedBlockAnns:
			if err := p.SendNewBlockHashes([]common.Hash{block.Hash()}, []uint64{block.NumberU64()}); err != nil {
				return
			}
			p.Log().Trace("Announced block", "number", block.Number(), "hash", block.Hash())

		case <-p.term:
			return
		}
	}
}

// broadcastTransactions is a write loop that schedules transaction broadcasts
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) broadcastTransactions() {
	var (
		queue  []common.Hash         // Queue of hashes to broadcast as full transactions
		done   chan struct{}         // Non-nil if background broadcaster is running
		fail   = make(chan error, 1) // Channel used to receive network error
		failed bool                  // Flag whether a send failed, discard everything onward
	)
	for {
		// If there's no in-flight broadcast running, check if a new one is needed
		if done == nil && len(queue) > 0 {
			// Pile transaction until we reach our allowed network limit
			var (
				hashesCount uint64
				txs         []*types.Transaction
				size        common.StorageSize
			)
			for i := 0; i < len(queue) && size < maxTxPacketSize; i++ {
				if tx := p.txpool.Get(queue[i]); tx != nil {
					txs = append(txs, tx)
					size += common.StorageSize(tx.Size())
				}
				hashesCount++
			}
			queue = queue[:copy(queue, queue[hashesCount:])]

			// If there's anything available to transfer, fire up an async writer
			if len(txs) > 0 {
				done = make(chan struct{})
				go func() {
					if err := p.SendTransactions(txs); err != nil {
						p.Log().Warn("Broadcast transactions failed", "peerId", p.ID(), "peerIP", safeGetPeerIP(p), "lost", len(txs), "hashes", concat(collectHashes(txs)), "err", err.Error())
						fail <- err
						return
					}
					close(done)
					p.Log().Trace("Sent transaction bodies", "count", len(txs), "peer.id", p.Node().ID().String(), "peer.ip", p.Node().IP().String())
				}()
			}
		}
		// Transfer goroutine may or may not have been started, listen for events
		select {
		case hashes := <-p.txBroadcast:
			// If the connection failed, discard all transaction events
			if failed {
				continue
			}
			// New batch of transactions to be broadcast, queue them (with cap)
			queue = append(queue, hashes...)
			if len(queue) > maxQueuedTxs {
				p.Log().Warn("Broadcast hashes abandon", "peerId", p.ID(), "peerIP", safeGetPeerIP(p), "abandon", len(queue)-maxQueuedTxs)
				txBroadcastAbandonMeter.Mark(int64(len(queue) - maxQueuedTxs))
				// Fancy copy and resize to ensure buffer doesn't grow indefinitely
				queue = queue[:copy(queue, queue[len(queue)-maxQueuedTxs:])]
			}

			txP2PBroadQueueGauge.Update(int64(len(queue)))

		case <-done:
			done = nil

		case <-fail:
			failed = true

		case <-p.term:
			return
		}
	}
}

// announceTransactions is a write loop that schedules transaction broadcasts
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) announceTransactions() {
	var (
		queue  []common.Hash         // Queue of hashes to announce as transaction stubs
		done   chan struct{}         // Non-nil if background announcer is running
		fail   = make(chan error, 1) // Channel used to receive network error
		failed bool                  // Flag whether a send failed, discard everything onward
	)
	for {
		// If there's no in-flight announce running, check if a new one is needed
		if done == nil && len(queue) > 0 {
			// Pile transaction hashes until we reach our allowed network limit
			var (
				count        int
				pending      []common.Hash
				pendingTypes []byte
				pendingSizes []uint32
				size         common.StorageSize
			)
			for count = 0; count < len(queue) && size < maxTxPacketSize; count++ {
				if tx := p.txpool.Get(queue[count]); tx != nil {
					pending = append(pending, queue[count])
					pendingTypes = append(pendingTypes, tx.Type())
					pendingSizes = append(pendingSizes, uint32(tx.Size()))
					size += common.HashLength
				}
			}
			// Shift and trim queue
			queue = queue[:copy(queue, queue[count:])]

			// If there's anything available to transfer, fire up an async writer
			if len(pending) > 0 {
				done = make(chan struct{})
				go func() {
					// TODO need check it eth68 condition
					if err := p.sendPooledTransactionHashes(pending, pendingTypes, pendingSizes); err != nil {
						p.Log().Warn("Announce hashes failed", "peerId", p.ID(), "peerIP", safeGetPeerIP(p), "lost", len(pending), "hashes", concat(pending), "err", err.Error())
						fail <- err
						return
					}
					close(done)
					p.Log().Trace("Sent transaction announcements", "count", len(pending), "peer.Id", p.ID(), "peer.IP", p.Node().IP().String())
				}()
			}
		}
		// Transfer goroutine may or may not have been started, listen for events
		select {
		case hashes := <-p.txAnnounce:
			// If the connection failed, discard all transaction events
			if failed {
				continue
			}
			// New batch of transactions to be broadcast, queue them (with cap)
			queue = append(queue, hashes...)
			if len(queue) > maxQueuedTxAnns {
				p.Log().Warn("Announce hashes abandon", "peerId", p.ID(), "peerIP", safeGetPeerIP(p), "abandon", len(queue)-maxQueuedTxAnns, "hashes", concat(queue[:len(queue)-maxQueuedTxAnns]))
				txAnnounceAbandonMeter.Mark(int64(len(queue) - maxQueuedTxAnns))
				// Fancy copy and resize to ensure buffer doesn't grow indefinitely
				queue = queue[:copy(queue, queue[len(queue)-maxQueuedTxAnns:])]
			}

			txP2PAnnQueueGauge.Update(int64(len(queue)))

		case <-done:
			done = nil

		case <-fail:
			failed = true

		case <-p.term:
			return
		}
	}
}

func collectHashes(txs []*types.Transaction) []common.Hash {
	hashes := make([]common.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash()
	}
	return hashes
}

func concat(hashes []common.Hash) string {
	strslice := make([]string, len(hashes))
	for i, hash := range hashes {
		strslice[i] = hash.String()
	}
	return strings.Join(strslice, ",")
}
