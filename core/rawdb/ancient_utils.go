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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type tableSize struct {
	name string
	size common.StorageSize
}

// freezerInfo contains the basic information of the freezer.
type freezerInfo struct {
	name  string      // The identifier of freezer
	head  uint64      // The number of last stored item in the freezer
	tail  uint64      // The number of first stored item in the freezer
	sizes []tableSize // The storage size per table
}

// count returns the number of stored items in the freezer.
func (info *freezerInfo) count() uint64 {
	return info.head - info.tail + 1
}

// size returns the storage size of the entire freezer.
func (info *freezerInfo) size() common.StorageSize {
	var total common.StorageSize
	for _, table := range info.sizes {
		total += table.size
	}
	return total
}

func inspect(name string, order map[string]bool, reader ethdb.AncientReader) (freezerInfo, error) {
	info := freezerInfo{name: name}
	for t := range order {
		size, err := reader.AncientSize(t)
		if err != nil {
			return freezerInfo{}, err
		}
		info.sizes = append(info.sizes, tableSize{name: t, size: common.StorageSize(size)})
	}
	// Retrieve the number of last stored item
	ancients, err := reader.Ancients()
	if err != nil {
		return freezerInfo{}, err
	}
	info.head = ancients - 1

	// Retrieve the number of first stored item
	tail, err := reader.Tail()
	if err != nil {
		return freezerInfo{}, err
	}
	info.tail = tail
	return info, nil
}

// inspectFreezers inspects all freezers registered in the system.
func inspectFreezers(db ethdb.Database) ([]freezerInfo, error) {
	var infos []freezerInfo
	for _, freezer := range freezers {
		switch freezer {
		case ChainFreezerName:
			info, err := inspect(ChainFreezerName, chainFreezerNoSnappy, db.BlockStore())
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)

		case StateFreezerName:
			if ReadStateScheme(db) != PathScheme {
				continue
			}
			datadir, err := db.StateStore().AncientDatadir()
			if err != nil {
				return nil, err
			}
			f, err := NewStateFreezer(datadir, true, DetectTrieNodesFile(datadir))
			if err != nil {
				return nil, err
			}
			defer f.Close()

			info, err := inspect(StateFreezerName, stateFreezerNoSnappy, f)
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)

		case ProofFreezerName:
			if ReadStateScheme(db) != PathScheme {
				continue
			}
			datadir, err := db.AncientDatadir()
			if err != nil {
				return nil, err
			}
			f, err := NewProofFreezer(datadir, true)
			if err != nil {
				log.Warn("If proof keeper is not enabled, there will be no ProofFreezer.")
				return nil, nil
			}
			defer f.Close()

			info, err := inspect(ProofFreezerName, proofFreezerNoSnappy, f)
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)

		default:
			return nil, fmt.Errorf("unknown freezer, supported ones: %v", freezers)
		}
	}
	return infos, nil
}

// InspectFreezerTable dumps out the index of a specific freezer table. The passed
// ancient indicates the path of root ancient directory where the chain freezer can
// be opened. Start and end specify the range for dumping out indexes.
// Note this function can only be used for debugging purposes.
func InspectFreezerTable(ancient string, freezerName string, tableName string, start, end int64, multiDatabase bool) error {
	var (
		path   string
		tables map[string]bool
	)
	switch freezerName {
	case ChainFreezerName:
		if multiDatabase {
			path, tables = resolveChainFreezerDir(filepath.Dir(ancient)+"/block/ancient"), chainFreezerNoSnappy
		} else {
			path, tables = resolveChainFreezerDir(ancient), chainFreezerNoSnappy
		}

	case StateFreezerName:
		if multiDatabase {
			path, tables = filepath.Join(filepath.Dir(ancient)+"/state/ancient", freezerName), stateFreezerNoSnappy
		} else {
			path, tables = filepath.Join(ancient, freezerName), stateFreezerNoSnappy
		}
	default:
		return fmt.Errorf("unknown freezer, supported ones: %v", freezers)
	}
	noSnappy, exist := tables[tableName]
	if !exist {
		var names []string
		for name := range tables {
			names = append(names, name)
		}
		return fmt.Errorf("unknown table, supported ones: %v", names)
	}
	table, err := newFreezerTable(path, tableName, noSnappy, true)
	if err != nil {
		return err
	}
	table.dumpIndexStdout(start, end)
	return nil
}

// DetectTrieNodesFile detects whether trie nodes data exists
func DetectTrieNodesFile(ancientDir string) bool {
	trieNodesFilePath := filepath.Join(ancientDir, StateFreezerName, fmt.Sprintf("%s.%s",
		stateHistoryTrieNodesData, "cidx"))
	return common.FileExist(trieNodesFilePath)
}

// DeleteTrieNodesFile deletes all trie nodes data in state directory
func DeleteTrieNodesFile(ancientDir string) error {
	statePath := filepath.Join(ancientDir, StateFreezerName)
	dir, err := os.Open(statePath)
	if err != nil {
		return err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	for _, name := range names {
		filePath := filepath.Join(statePath, name)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		if !fileInfo.IsDir() && strings.Contains(filepath.Base(filePath), stateHistoryTrieNodesData) {
			err = os.Remove(filePath)
			if err != nil {
				return err
			}
			log.Info(fmt.Sprintf("Delete %s file", filePath))
		}
	}

	return nil
}
