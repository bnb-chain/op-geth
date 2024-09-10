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

// FixResult holds the result of the fix operation
type FixResult struct {
	Success bool
	Err     error
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
		resultChan := make(chan FixResult, 1) // Channel to capture fix result (success or error)
		fm.fixChannels.Store(id, resultChan)

		go func() {
			defer func() {
				fm.mutex.Lock()
				fm.isFixInProgress = false
				fm.mutex.Unlock()

				if ch, ok := fm.fixChannels.Load(id); ok {
					resultChan := ch.(chan FixResult)
					close(resultChan)
				}
			}()
			worker.fix(parentHash, resultChan) // processing fix logic
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
		log.Info("Payload is not fixing or has been completed")
		return
	}

	// Check if a listener goroutine has already been started
	if _, listenerExists := fm.listenerStarted.LoadOrStore(id, true); listenerExists {
		log.Info("Listener already started for payload", "payload", id)
		return
	}

	go func() {
		log.Info("Start waiting for fix completion")
		result := <-ch.(chan FixResult) // Wait for the fix result

		// Check the result and decide whether to retry the payload update
		if result.Success {
			if err := worker.retryPayloadUpdate(args, payload); err != nil {
				log.Error("Failed to retry payload update after fix", "id", id, "err", err)
			} else {
				log.Info("Payload update after fix succeeded", "id", id)
			}
		} else {
			log.Error("Fix failed, skipping payload update", "id", id, "err", result.Err)
		}

		// Clean up the fix state
		fm.fixChannels.Delete(id)
		fm.listenerStarted.Delete(id)
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

	log.Info("Fixing data for block", "block number", block.NumberU64())
	latestValid, err := w.chain.RecoverStateAndSetHead(block)
	if err != nil {
		return fmt.Errorf("failed to recover state: %v", err)
	}

	log.Info("Recovered states up to block", "latestValid", latestValid)
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
