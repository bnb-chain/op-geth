// Copyright 2026 The go-ethereum Authors
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

package core

import (
	"math/big"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

// TestAlignHeadsAfterStateRepair covers the startup repair path where the
// executable head is rewound below the canonical header chain, leaving the
// blocks in between canonical but stateless. The gap has to be closed by
// re-executing those blocks; deleting them would take away the material a
// sequencer needs to restore its own head after an unclean shutdown.
func TestAlignHeadsAfterStateRepair(t *testing.T) {
	const chainLength = 10

	tests := []struct {
		name          string
		head          uint64
		safe          uint64
		stateRepaired bool
		vanillaGeth   bool
		statelessHead bool
		wantHead      uint64
	}{
		{
			name:          "repaired head behind header chain rolls forward",
			head:          6,
			safe:          8,
			stateRepaired: true,
			wantHead:      chainLength,
		},
		{
			name:          "no repair during startup",
			head:          6,
			safe:          8,
			stateRepaired: false,
			wantHead:      6,
		},
		{
			name:          "executable head at genesis",
			head:          0,
			safe:          8,
			stateRepaired: true,
			wantHead:      0,
		},
		{
			name:          "header chain already aligned",
			head:          chainLength,
			safe:          chainLength,
			stateRepaired: true,
			wantHead:      chainLength,
		},
		{
			// Vanilla geth is legitimately behind its header chain and refills
			// the gap through the beacon downloader instead.
			name:          "non-optimism chain",
			head:          6,
			safe:          8,
			stateRepaired: true,
			vanillaGeth:   true,
			wantHead:      6,
		},
		{
			// A node whose head state is still missing after the repair has
			// nothing to re-execute from and is waiting for state sync.
			name:          "head state missing after repair",
			head:          6,
			safe:          8,
			stateRepaired: true,
			statelessHead: true,
			wantHead:      6,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bc := newPathSchemeTestChain(t, chainLength)
			defer bc.Stop()
			if !test.vanillaGeth {
				bc.chainConfig.Optimism = testOptimismConfig()
			}

			// Inserting the chain left every marker at its tip. The crash being
			// simulated only rewinds the executable head below them.
			if got := bc.CurrentSnapBlock().Number.Uint64(); got != chainLength {
				t.Fatalf("snap marker starts at %d, want %d", got, chainLength)
			}
			head := bc.GetHeaderByNumber(test.head)
			if test.statelessHead {
				stateless := types.CopyHeader(head)
				stateless.Root = common.HexToHash("0xdead")
				head = stateless
			}
			bc.currentBlock.Store(head)
			rawdb.WriteHeadBlockHash(bc.db.BlockStore(), head.Hash())
			safe := bc.GetHeaderByNumber(test.safe)
			bc.SetSafe(safe)
			bc.SetFinalized(safe)

			bc.alignHeadsAfterStateRepair(test.stateRepaired)

			// The header chain is never touched, whichever branch was taken.
			if got := bc.CurrentHeader().Number.Uint64(); got != chainLength {
				t.Fatalf("header head is %d, want %d", got, chainLength)
			}
			if got := bc.CurrentBlock().Number.Uint64(); got != test.wantHead {
				t.Fatalf("executable head is %d, want %d", got, test.wantHead)
			}
			// No block may be dropped: the sequencer recovery path in the miner
			// can only roll forward over blocks that are still present.
			for number := uint64(1); number <= chainLength; number++ {
				if bc.GetBlockByNumber(number) == nil {
					t.Fatalf("block %d was deleted from the canonical chain", number)
				}
			}
			// The snap marker is left where it was, still pointing at a block
			// that exists.
			snap := bc.CurrentSnapBlock()
			if snap == nil {
				t.Fatal("snap marker is nil")
			}
			if got := snap.Number.Uint64(); got != chainLength {
				t.Fatalf("snap marker is %d, want %d", got, chainLength)
			}
			if got := rawdb.ReadHeadFastBlockHash(bc.db); got != snap.Hash() {
				t.Fatalf("persisted snap marker is %s, want %s", got, snap.Hash())
			}
			// The markers may never end up above the executable head.
			wantMarker := min(test.safe, test.wantHead)
			assertHeaderNumber(t, "safe", bc.CurrentSafeBlock(), &wantMarker)
			assertHeaderNumber(t, "finalized", bc.CurrentFinalBlock(), &wantMarker)
		})
	}
}

// TestAlignHeadsAfterStateRepairReExecutesMissingState is the case the roll
// forward exists for: a force kill loses the in-memory state layers, startup
// rewinds the executable head to the last persisted one, and the blocks above
// it have to be run again rather than reported to op-node as already known.
//
// The vanilla run is the control. It takes the identical crash and shows the
// gap the Optimism run has to close, so a passing Optimism run cannot be the
// result of the crash failing to produce a gap in the first place.
func TestAlignHeadsAfterStateRepairReExecutesMissingState(t *testing.T) {
	const persisted = 6
	const chainLength = 10

	for _, snapshots := range []bool{false, true} {
		name := "without snapshots"
		if snapshots {
			name = "with snapshots"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("optimism rolls the head forward", func(t *testing.T) {
				repaired := restartAfterForceKill(t, true, snapshots, persisted, chainLength)
				if got := repaired.CurrentBlock().Number.Uint64(); got != chainLength {
					t.Fatalf("executable head is %d, want %d", got, chainLength)
				}
				if !repaired.HasState(repaired.CurrentBlock().Root) {
					t.Fatal("executable head has no state after re-execution")
				}
			})

			t.Run("vanilla geth keeps the gap", func(t *testing.T) {
				repaired := restartAfterForceKill(t, false, snapshots, persisted, chainLength)
				if got := repaired.CurrentBlock().Number.Uint64(); got != persisted {
					t.Fatalf("executable head is %d, want %d", got, persisted)
				}
				if got := repaired.CurrentHeader().Number.Uint64(); got != chainLength {
					t.Fatalf("header head is %d, want %d", got, chainLength)
				}
			})
		})
	}
}

// restartAfterForceKill builds a chain whose state is only persisted up to
// persisted, drops the rest the way a force kill does, and returns the chain
// that comes back up over the same database.
//
// With snapshots enabled the snapshot has to be flushed to the same block as
// the trie. It is an independent persistence point, and if it is left behind
// the repair rewinds to wherever it sits rather than to persisted.
func restartAfterForceKill(t *testing.T, optimism, snapshots bool, persisted, chainLength int) *BlockChain {
	t.Helper()

	// Re-execution validates headers, so the chain has to be generated under
	// the same fee rules it is later validated with.
	config := *params.AllEthashProtocolChanges
	if optimism {
		config.Optimism = testOptimismConfig()
	}
	genesis := &Genesis{BaseFee: big.NewInt(params.InitialBaseFee), Config: &config}
	engine := ethash.NewFaker()
	// The synchronous node buffer makes the commit below land on disk before
	// the next insert.
	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.PathNodeBuffer = pathdb.SyncNodeBuffer
	if !snapshots {
		cacheConfig.SnapshotLimit = 0
	}

	// The repair path consults the state history freezer, so the database has
	// to be a real one rather than purely in memory.
	datadir := t.TempDir()
	ancient := filepath.Join(datadir, "ancient")
	openDB := func() ethdb.Database {
		t.Helper()
		db, err := rawdb.Open(rawdb.OpenOptions{Directory: datadir, AncientsDirectory: ancient, Ephemeral: true})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		return db
	}

	db := openDB()
	bc, err := NewBlockChain(db, cacheConfig, genesis, nil, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create chain: %v", err)
	}
	_, blocks := makeBlockChainWithGenesis(genesis, chainLength, engine, canonicalSeed)
	if _, err := bc.InsertChain(blocks[:persisted]); err != nil {
		t.Fatalf("failed to insert persisted segment: %v", err)
	}
	// Flush so that the restart finds state for this block and no later one.
	persistedRoot := bc.CurrentBlock().Root
	if err := bc.triedb.Commit(persistedRoot, false); err != nil {
		t.Fatalf("failed to commit state: %v", err)
	}
	if bc.snaps != nil {
		if err := bc.snaps.Cap(persistedRoot, 0); err != nil {
			t.Fatalf("failed to flush snapshot: %v", err)
		}
	}
	if _, err := bc.InsertChain(blocks[persisted:]); err != nil {
		t.Fatalf("failed to insert unflushed segment: %v", err)
	}
	// A force kill never gets to journal the layers it still holds in memory.
	db.Close()
	bc.stopWithoutSaving()
	bc.triedb.Close()

	newdb := openDB()
	t.Cleanup(func() { newdb.Close() })
	repaired, err := NewBlockChain(newdb, cacheConfig, genesis, nil, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to restart chain: %v", err)
	}
	t.Cleanup(repaired.Stop)
	return repaired
}

func TestClampHeadMarkersToCurrentBlock(t *testing.T) {
	tests := []struct {
		name          string
		head          uint64
		safe          *uint64
		finalized     *uint64
		wantSafe      *uint64
		wantFinalized *uint64
	}{
		{
			name:          "both markers ahead",
			head:          10,
			safe:          uint64Ptr(12),
			finalized:     uint64Ptr(11),
			wantSafe:      uint64Ptr(10),
			wantFinalized: uint64Ptr(10),
		},
		{
			name:          "only safe ahead",
			head:          10,
			safe:          uint64Ptr(12),
			wantSafe:      uint64Ptr(10),
			wantFinalized: nil,
		},
		{
			name:          "only finalized ahead",
			head:          10,
			finalized:     uint64Ptr(12),
			wantSafe:      nil,
			wantFinalized: uint64Ptr(10),
		},
		{
			name:          "markers already valid",
			head:          10,
			safe:          uint64Ptr(9),
			finalized:     uint64Ptr(8),
			wantSafe:      uint64Ptr(9),
			wantFinalized: uint64Ptr(8),
		},
		{
			name: "nil markers",
			head: 10,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bc := &BlockChain{db: rawdb.NewMemoryDatabase()}
			bc.currentBlock.Store(testHeader(test.head))
			if test.safe != nil {
				bc.currentSafeBlock.Store(testHeader(*test.safe))
			}
			if test.finalized != nil {
				finalized := testHeader(*test.finalized)
				bc.currentFinalBlock.Store(finalized)
				rawdb.WriteFinalizedBlockHash(bc.db.BlockStore(), finalized.Hash())
			}

			bc.clampHeadMarkersToCurrentBlock()

			assertHeaderNumber(t, "safe", bc.CurrentSafeBlock(), test.wantSafe)
			assertHeaderNumber(t, "finalized", bc.CurrentFinalBlock(), test.wantFinalized)
			var wantFinalizedHash common.Hash
			if test.wantFinalized != nil {
				wantFinalizedHash = testHeader(*test.wantFinalized).Hash()
			}
			if got := rawdb.ReadFinalizedBlockHash(bc.db.BlockStore()); got != wantFinalizedHash {
				t.Fatalf("persisted finalized marker is %s, want %s", got, wantFinalizedHash)
			}
		})
	}
}

// newPathSchemeTestChain builds a canonical path-scheme chain whose chain config
// is private to the caller, so that a test may enable Optimism rules on it
// without generating the chain under those rules.
func newPathSchemeTestChain(t *testing.T, length int) *BlockChain {
	t.Helper()

	config := *params.AllEthashProtocolChanges
	genesis := &Genesis{BaseFee: big.NewInt(params.InitialBaseFee), Config: &config}
	engine := ethash.NewFaker()

	bc, err := NewBlockChain(rawdb.NewMemoryDatabase(), DefaultCacheConfigWithScheme(rawdb.PathScheme), genesis, nil, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create chain: %v", err)
	}
	_, blocks := makeBlockChainWithGenesis(genesis, length, engine, canonicalSeed)
	if _, err := bc.InsertChain(blocks); err != nil {
		bc.Stop()
		t.Fatalf("failed to insert chain: %v", err)
	}
	return bc
}

// testOptimismConfig marks a chain as Optimism with fee parameters that survive
// header verification, which re-execution performs.
func testOptimismConfig() *params.OptimismConfig {
	return &params.OptimismConfig{EIP1559Elasticity: 6, EIP1559Denominator: 50}
}

func testHeader(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number)}
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func assertHeaderNumber(t *testing.T, marker string, header *types.Header, want *uint64) {
	t.Helper()
	if want == nil {
		if header != nil {
			t.Fatalf("%s marker is set to %d, want nil", marker, header.Number.Uint64())
		}
		return
	}
	if header == nil {
		t.Fatalf("%s marker is nil, want %d", marker, *want)
	}
	if got := header.Number.Uint64(); got != *want {
		t.Fatalf("%s marker is %d, want %d", marker, got, *want)
	}
}
