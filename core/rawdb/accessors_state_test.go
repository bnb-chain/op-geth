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

package rawdb

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

// Keep the real freezer transaction and rollback while failing one append.
type failingHistoryWriter struct {
	ethdb.AncientWriter
	table string
	err   error
}

func (w *failingHistoryWriter) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return w.AncientWriter.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		return fn(&failingHistoryWriteOp{AncientWriteOp: op, table: w.table, err: w.err})
	})
}

type failingHistoryWriteOp struct {
	ethdb.AncientWriteOp
	table string
	err   error
}

func (op *failingHistoryWriteOp) AppendRaw(table string, number uint64, data []byte) error {
	if table == op.table {
		return op.err
	}
	return op.AncientWriteOp.AppendRaw(table, number, data)
}

func TestWriteStateHistory(t *testing.T) {
	for name, fastRecovery := range map[string]bool{"plain": false, "trie-nodes": true} {
		t.Run(name, func(t *testing.T) {
			data := []byte("history")
			write := func(writer ethdb.AncientWriter, id uint64) error {
				if fastRecovery {
					return WriteStateHistoryWithTrieNodes(writer, id, data, data, data, data, data, data)
				}
				return WriteStateHistory(writer, id, data, data, data, data, data)
			}
			open := func(t *testing.T) *ResettableFreezer {
				t.Helper()
				f, err := NewStateFreezer(t.TempDir(), false, fastRecovery)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := f.Close(); err != nil {
						t.Error(err)
					}
				})
				return f
			}
			tables := []string{stateHistoryMeta, stateHistoryAccountIndex, stateHistoryStorageIndex, stateHistoryAccountData, stateHistoryStorageData}
			if fastRecovery {
				tables = append(tables, stateHistoryTrieNodesData)
			}
			for _, table := range tables {
				t.Run("append-error/"+table, func(t *testing.T) {
					f := open(t)
					want := errors.New("injected append failure")
					writer := &failingHistoryWriter{AncientWriter: f, table: table, err: want}
					if err := write(writer, 1); !errors.Is(err, want) {
						t.Fatalf("write error = %v, want %v", err, want)
					}
					if head, err := f.Ancients(); err != nil || head != 0 {
						t.Fatalf("head = %d, err = %v", head, err)
					}
					// Retrying without the injected append failure must find every table
					// rolled back to the same position.
					if err := write(f, 1); err != nil {
						t.Fatalf("write after rollback: %v", err)
					}
				})
			}
			t.Run("commit-error", func(t *testing.T) {
				f := open(t)
				table := f.freezer.tables[stateHistoryMeta]
				original := table.head
				readonly, err := os.Open(original.Name())
				if err != nil {
					t.Fatal(err)
				}
				table.head = readonly
				t.Cleanup(func() { table.head = original; readonly.Close() })
				// AppendRaw buffers these small blobs successfully. The actual file
				// write in ModifyAncients' final batch commit fails on the read-only FD.
				if err := write(f, 1); err == nil {
					t.Fatal("write succeeded with an unwritable data file")
				}
				if head, err := f.Ancients(); err != nil || head != 0 {
					t.Fatalf("head = %d, err = %v", head, err)
				}
			})
			t.Run("read-only", func(t *testing.T) {
				dir := t.TempDir()
				f, err := NewStateFreezer(dir, false, fastRecovery)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				f, err = NewStateFreezer(dir, true, fastRecovery)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err := write(f, 1); !errors.Is(err, errReadOnly) {
					t.Fatalf("write error = %v, want %v", err, errReadOnly)
				}
			})
			t.Run("success-and-out-of-order", func(t *testing.T) {
				f := open(t)
				if err := write(f, 1); err != nil {
					t.Fatal(err)
				}
				for _, table := range tables {
					blob, err := f.Ancient(table, 0)
					if err != nil || !bytes.Equal(blob, data) {
						t.Fatalf("table %s: data %x, error %v", table, blob, err)
					}
				}
				for _, id := range []uint64{1, 3} {
					if err := write(f, id); !errors.Is(err, errOutOrderInsertion) {
						t.Fatalf("write id %d: error = %v, want %v", id, err, errOutOrderInsertion)
					}
				}
				if head, err := f.Ancients(); err != nil || head != 1 {
					t.Fatalf("head = %d, err = %v", head, err)
				}
				if err := write(f, 2); err != nil {
					t.Fatalf("sequential write after rejected writes: %v", err)
				}
			})
		})
	}
}
