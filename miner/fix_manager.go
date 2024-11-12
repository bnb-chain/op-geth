package miner

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// StateFixManager manages the fix operation state and notification mechanism.
type StateFixManager struct {
	mutex           sync.Mutex // Protects access to fix state
	isFixInProgress bool       // Tracks if a fix operation is in progress
}

// NewFixManager initializes a FixManager with required dependencies
func NewFixManager() *StateFixManager {
	return &StateFixManager{}
}

// StartFix launches a goroutine to manage the fix process and tracks the fix state.
func (fm *StateFixManager) StartFix(worker *worker, id engine.PayloadID, parentHash common.Hash) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if fm.isFixInProgress {
		log.Warn("Fix is already in progress for this block", "id", id)
		return nil
	}

	fm.isFixInProgress = true
	defer func() {
		fm.isFixInProgress = false
	}()

	log.Info("Starting synchronous fix process", "id", id)
	err := worker.fix(parentHash)
	if err != nil {
		log.Error("Fix process failed", "error", err)
		return err
	}

	log.Info("Fix process completed successfully", "id", id)
	return nil
}

// RecoverFromLocal attempts to recover the block and MPT data from the local chain.
//
// blockHash: The latest header(unsafe block) hash of the block to recover.
func (fm *StateFixManager) RecoverFromLocal(w *worker, blockHash common.Hash) error {
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
