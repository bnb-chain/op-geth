// Copyright 2024 The go-ethereum Authors
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

package pathdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/testutil"
)

// TestCreateBlockIntervalBoundaryStartPreservesProposerBlock ensures that a
// recovery starting exactly on a dlInMd boundary yields a layer ending on that
// boundary, which proposedBlockReader requires to serve the block's state.
func TestCreateBlockIntervalBoundaryStartPreservesProposerBlock(t *testing.T) {
	nf := &nodebufferlist{dlInMd: 1800}

	got := nf.createBlockInterval(3600, 7200)
	want := [][]uint64{
		{5401, 7200},
		{3601, 5400},
		{3600, 3600},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("createBlockInterval(3600, 7200) = %v, want %v", got, want)
	}
}

func TestCreateBlockIntervalNonAlignedStart(t *testing.T) {
	nf := &nodebufferlist{dlInMd: 1800}

	got := nf.createBlockInterval(3601, 7200)
	want := [][]uint64{
		{5401, 7200},
		{3601, 5400},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("createBlockInterval(3601, 7200) = %v, want %v", got, want)
	}
}

// TestRecoverNodeBufferListDoesNotBlockOnKeepFunc reproduces the startup
// deadlock: ProofKeeper.GetNotifyKeepRecordFunc sends on an unbuffered channel
// whose receiver (eventLoop) is only started after NewBlockChain returns, but
// recoverNodeBufferList runs inside NewBlockChain → triedb.New. Calling
// keepFunc from the synchronous diffToBase therefore blocks forever.
func TestRecoverNodeBufferListDoesNotBlockOnKeepFunc(t *testing.T) {
	const (
		histories = 5
		wpBlocks  = 2 // yields rsevMdNum=3, dlInMd=1, so every block is its own layer
		limit     = 1024 * 1024
	)

	freezer, err := rawdb.NewStateFreezer(t.TempDir(), false, true)
	if err != nil {
		t.Fatalf("Failed to open freezer: %v", err)
	}
	defer freezer.Close()

	parent := types.EmptyRootHash
	for i := uint64(1); i <= histories; i++ {
		root := testutil.RandomHash()
		h := newHistory(root, parent, i, randomStateSet(1), randomTrieNodes(1))
		accountData, storageData, accountIndex, storageIndex, trieNodes := h.encode()
		rawdb.WriteStateHistoryWithTrieNodes(freezer, i, h.meta.encode(),
			accountIndex, storageIndex, accountData, storageData, trieNodes)
		parent = root
	}

	// Unbuffered send with no receiver: the production keepFunc shape.
	keepCh := make(chan *KeepRecord)
	keepFunc := func(record *KeepRecord) { keepCh <- record }

	done := make(chan error, 1)
	var nbl *nodebufferlist
	go func() {
		var recErr error
		nbl, recErr = newNodeBufferList(rawdb.NewMemoryDatabase(), limit, nil, 0, wpBlocks,
			keepFunc, freezer, true, false)
		done <- recErr
	}()

	select {
	case recErr := <-done:
		if recErr != nil {
			t.Fatalf("Failed to recover node buffer list: %v", recErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recoverNodeBufferList blocked on keepFunc; ProofKeeper event loop is not running during recovery")
	}
	if nbl.keepFunc == nil {
		t.Fatal("keepFunc was dropped by the recovery path")
	}
}
