package miner

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/log"
)

// FixManager manages the fix operation state and notification mechanism.
type FixManager struct {
	mutex           sync.Mutex             // Protects access to fix state
	isFixInProgress bool                   // Tracks if a fix operation is in progress
	fixChannels     sync.Map               // Stores fix state and notification channels
	listenerStarted sync.Map               // Tracks whether a listener goroutine has started for each payload ID
	downloader      *downloader.Downloader // Used to trigger BeaconSync operations

}

// NewFixManager initializes a FixManager with required dependencies
func NewFixManager(downloader *downloader.Downloader) *FixManager {
	return &FixManager{
		downloader: downloader,
	}
}

// StartFix launches a goroutine to manage the fix process and tracks the fix state.
func (fm *FixManager) StartFix(worker *worker, id engine.PayloadID, parentHash common.Hash) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if !fm.isFixInProgress {
		fm.isFixInProgress = true
		fixChan := make(chan struct{})
		fm.fixChannels.Store(id, fixChan)

		go func() {
			defer func() {
				fm.mutex.Lock()
				fm.isFixInProgress = false
				fm.mutex.Unlock()

				// Notify listeners that the fix is complete
				if ch, ok := fm.fixChannels.Load(id); ok {
					close(ch.(chan struct{}))
				}
			}()
			worker.fix(parentHash) // Execute the fix logic
		}()
	}
}

// ListenFixCompletion listens for the completion of the fix process to avoid redundant goroutine starts.
//
// payload: The payload that will be updated after fix completion.
// args: The arguments required to retry the payload update.
func (fm *FixManager) ListenFixCompletion(worker *worker, id engine.PayloadID, payload *Payload, args *BuildPayloadArgs) {
	ch, exists := fm.fixChannels.Load(id)
	if !exists {
		log.Info("payload is not fixing or has been completed")
		return
	}

	// Check if a listener goroutine has already been started
	if _, listenerExists := fm.listenerStarted.LoadOrStore(id, true); listenerExists {
		log.Info("Listener already started for payload", "payload", id)
		return // If listener goroutine already exists, return immediately
	}

	go func() {
		log.Info("start waiting")
		<-ch.(chan struct{}) // Wait for the fix to complete
		log.Info("Fix completed, retrying payload update", "id", id)
		worker.retryPayloadUpdate(args, payload)
		fm.fixChannels.Delete(id)     // Remove the id from fixChannels
		fm.listenerStarted.Delete(id) // Remove the listener flag for this id
	}()
}

// RecoverFromLocal attempts to recover the block and MPT data from the local chain.
//
// blockHash: The latest header(unsafe block) hash of the block to recover.
func (fm *FixManager) RecoverFromLocal(w *worker, blockHash common.Hash) error {
	block := w.chain.GetBlockByHash(blockHash)
	if block == nil {
		return fmt.Errorf("block not found in local chain")
	}

	log.Info("Fixing data for block", "blocknumber", block.NumberU64())
	latestValid, err := w.chain.RecoverAncestors(block)
	if err != nil {
		return fmt.Errorf("failed to recover ancestors: %v", err)
	}

	log.Info("Recovered ancestors up to block", "latestValid", latestValid)
	return nil
}

// RecoverFromPeer attempts to retrieve the block header from peers and triggers BeaconSync if successful.
//
// blockHash: The latest header(unsafe block) hash of the block to recover.
func (fm *FixManager) RecoverFromPeer(blockHash common.Hash) error {
	peers := fm.downloader.GetAllPeers()
	if len(peers) == 0 {
		return fmt.Errorf("no peers available")
	}

	var header *types.Header
	var err error
	for _, peer := range peers {
		header, err = fm.downloader.GetHeaderByHashFromPeer(peer, blockHash)
		if err == nil && header != nil {
			break
		}
		log.Warn("Failed to retrieve header from peer", "err", err)
	}

	if header == nil {
		return fmt.Errorf("failed to retrieve header from  all valid peers")
	}

	log.Info("Successfully retrieved header from peer", "blockHash", blockHash)

	fm.downloader.BeaconSync(downloader.FullSync, header, nil)
	return nil
}
