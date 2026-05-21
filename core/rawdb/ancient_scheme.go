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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/log"
)

// The list of table names of chain freezer.
const (
	// ChainFreezerHeaderTable indicates the name of the freezer header table.
	ChainFreezerHeaderTable = "headers"

	// ChainFreezerHashTable indicates the name of the freezer canonical hash table.
	ChainFreezerHashTable = "hashes"

	// ChainFreezerBodiesTable indicates the name of the freezer block body table.
	ChainFreezerBodiesTable = "bodies"

	// ChainFreezerReceiptTable indicates the name of the freezer receipts table.
	ChainFreezerReceiptTable = "receipts"

	// ChainFreezerDifficultyTable indicates the name of the freezer total difficulty table.
	ChainFreezerDifficultyTable = "diffs"
)

// chainFreezerTableConfigs configures the settings for tables in the chain freezer.
// Compression is disabled for hashes as they don't compress well. Additionally,
// tail truncation is disabled for the header and hash tables, as these are intended
// to be retained long-term.
var chainFreezerTableConfigs = map[string]freezerTableConfig{
	ChainFreezerHeaderTable:     {noSnappy: false, prunable: true},
	ChainFreezerHashTable:       {noSnappy: true, prunable: true},
	ChainFreezerBodiesTable:     {noSnappy: false, prunable: true},
	ChainFreezerReceiptTable:    {noSnappy: false, prunable: true},
	ChainFreezerDifficultyTable: {noSnappy: true, prunable: true},
}

// freezerTableConfig contains the settings for a freezer table.
type freezerTableConfig struct {
	noSnappy bool // disables item compression
	prunable bool // true for tables that can be pruned by TruncateTail
}

const (
	// stateHistoryTableSize defines the maximum size of freezer data files.
	stateHistoryTableSize = 2 * 1000 * 1000 * 1000

	// stateHistoryAccountIndex indicates the name of the freezer state history table.
	stateHistoryMeta         = "history.meta"
	stateHistoryAccountIndex = "account.index"
	stateHistoryStorageIndex = "storage.index"
	stateHistoryAccountData  = "account.data"
	stateHistoryStorageData  = "storage.data"

	// Used to fast recovery, shouble be deleted after supporting pbsss archive mode.
	stateHistoryTrieNodesData = "trienodes.data"
)

// stateFreezerTableConfigs configures the settings for tables in the state freezer.
var stateFreezerTableConfigs = map[string]freezerTableConfig{
	stateHistoryMeta:         {noSnappy: true, prunable: true},
	stateHistoryAccountIndex: {noSnappy: false, prunable: true},
	stateHistoryStorageIndex: {noSnappy: false, prunable: true},
	stateHistoryAccountData:  {noSnappy: false, prunable: true},
	stateHistoryStorageData:  {noSnappy: false, prunable: true},
}

const (
	trienodeHistoryHeaderTable       = "trienode.header"
	trienodeHistoryKeySectionTable   = "trienode.key"
	trienodeHistoryValueSectionTable = "trienode.value"
)

// trienodeFreezerTableConfigs configures the settings for tables in the trienode freezer.
var trienodeFreezerTableConfigs = map[string]freezerTableConfig{
	trienodeHistoryHeaderTable: {noSnappy: false, prunable: true},

	// Disable snappy compression to allow efficient partial read.
	trienodeHistoryKeySectionTable: {noSnappy: true, prunable: true},

	// Disable snappy compression to allow efficient partial read.
	trienodeHistoryValueSectionTable: {noSnappy: true, prunable: true},
}

// The list of identifiers of ancient stores.
var (
	ChainFreezerName          = "chain"    // the folder name of chain segment ancient store.
	MerkleStateFreezerName    = "state"    // the folder name of reverse diff ancient store.
	MerkleTrienodeFreezerName = "trienode" // the folder name of trienode history ancient store.

	// Used to get withdraw proof, shouble be deleted after supporting pbsss archive mode.
	ProofFreezerName = "proof" // the folder name of propose withdraw proof store.
)

// freezers the collections of all builtin freezers.
var freezers = []string{ChainFreezerName, MerkleStateFreezerName, MerkleTrienodeFreezerName}

// CleanupUnusedAncientStores removes legacy ancient data that is no longer
// used after PBSS archive mode: the proof freezer directory and trie nodes
// data files in the state freezer.
func CleanupUnusedAncientStores(ancientDir string) error {
	proofPath := filepath.Join(ancientDir, ProofFreezerName)
	if info, err := os.Stat(proofPath); err == nil && info.IsDir() {
		if err := os.RemoveAll(proofPath); err != nil {
			return err
		}
		log.Info("Removed unused ancient proof store", "path", proofPath)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	statePath := filepath.Join(ancientDir, MerkleStateFreezerName)
	info, err := os.Stat(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return nil
	}
	entries, err := os.ReadDir(statePath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.Contains(entry.Name(), "trienodes") {
			continue
		}
		filePath := filepath.Join(statePath, entry.Name())
		if err := os.Remove(filePath); err != nil {
			return err
		}
		log.Info("Removed unused trienodes ancient file", "path", filePath)
	}
	return nil
}

// NewStateFreezer initializes the ancient store for state history.
//
//   - if the empty directory is given, initializes the pure in-memory
//     state freezer (e.g. dev mode).
//   - if non-empty directory is given, initializes the regular file-based
//     state freezer.
func NewStateFreezer(ancientDir string, readOnly bool) (*ResettableFreezer, error) {
	if err := CleanupUnusedAncientStores(ancientDir); err != nil {
		log.Crit("Failed to cleanup unused ancient stores", "error", err)
	}
	return NewResettableFreezer(filepath.Join(ancientDir, MerkleStateFreezerName), "eth/db/state", readOnly, stateHistoryTableSize, stateFreezerTableConfigs)
}

// NewTrienodeFreezer initializes the ancient store for trienode history.
//
//   - if the empty directory is given, initializes the pure in-memory
//     trienode freezer (e.g. dev mode).
//   - if non-empty directory is given, initializes the regular file-based
//     trienode freezer.
func NewTrienodeFreezer(ancientDir string, readOnly bool) (*ResettableFreezer, error) {
	if err := CleanupUnusedAncientStores(ancientDir); err != nil {
		log.Crit("Failed to cleanup unused ancient stores", "error", err)
	}
	return NewResettableFreezer(filepath.Join(ancientDir, MerkleTrienodeFreezerName), "eth/db/trienode", readOnly, stateHistoryTableSize, trienodeFreezerTableConfigs)
}
