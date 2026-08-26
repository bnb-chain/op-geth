package pathdb

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/testutil"
)

func randomJournalNodes(n int) []journalNodes {
	jns := make([]journalNodes, 0, n)
	for i := 0; i < n; i++ {
		jn := make([]journalNode, 0, n)
		for j := 0; j < n; j++ {
			jn = append(jn, journalNode{
				Path: testutil.RandBytes(n),
				Blob: testutil.RandBytes(n),
			})
		}
		jns = append(jns, journalNodes{
			Owner: testutil.RandomHash(),
			Nodes: jn,
		})
	}
	return jns
}

func TestCompressTrieNodes(t *testing.T) {
	trieNodes := randomTrieNodes(3)
	jn := compressTrieNodes(trieNodes)
	assert.Equal(t, 1, len(jn))
}

func TestFlattenTrieNodes(t *testing.T) {
	jn := flattenTrieNodes(randomJournalNodes(3))
	assert.Equal(t, 3, len(jn))
}

// Every journal load failure ends with the journal discarded, so all of them
// must fall back to state history. The cases below enumerate what the journal
// layer can return, so reintroducing a filter on specific errors fails here.
func TestRecoverFromStateHistoryOnEveryJournalError(t *testing.T) {
	db := &Database{
		fastRecovery: true,
		config:       &Config{TrieNodeBufferType: NodeBufferList},
	}
	tests := []struct {
		name string
		err  error
	}{
		{"journal missing", errMissJournal},
		{"journal unmatched", fmt.Errorf("%w want %x got %x", errUnmatchedJournal, common.Hash{1}, common.Hash{2})},
		{"version missing", errMissVersion},
		{"version unexpected", fmt.Errorf("%w want %d got %d", errUnexpectedVersion, 0, 1)},
		{"disk root missing", errMissDiskRoot},
		{"disk layer truncated", fmt.Errorf("failed to load disk journal: %v", io.EOF)},
		{"disk nodes truncated", fmt.Errorf("failed to load disk nodes: %v", io.ErrUnexpectedEOF)},
		{"diff layer truncated", fmt.Errorf("failed to load diff root: %v", io.ErrUnexpectedEOF)},
		{"checksum mismatch", fmt.Errorf("expected shaSum: %v, real:%v", [32]byte{1}, [32]byte{2})},
		{"state id behind disk", fmt.Errorf("invalid state id: stored %d resolved %d", 10, 5)},
		{"journal file unreadable", fmt.Errorf("open journal: %v", os.ErrPermission)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, db.shouldRecoverFromStateHistory(test.err))
		})
	}
}

func TestSkipStateHistoryRecovery(t *testing.T) {
	tests := []struct {
		name string
		db   *Database
		err  error
	}{
		{
			name: "journal loaded successfully",
			db:   &Database{fastRecovery: true, config: &Config{TrieNodeBufferType: NodeBufferList}},
			err:  nil,
		},
		{
			name: "ancient db holds no trie nodes",
			db:   &Database{fastRecovery: false, config: &Config{TrieNodeBufferType: NodeBufferList}},
			err:  errMissJournal,
		},
		{
			name: "buffer is not a node buffer list",
			db:   &Database{fastRecovery: true, config: &Config{TrieNodeBufferType: AsyncNodeBuffer}},
			err:  errMissJournal,
		},
		{
			name: "buffer runs in base only mode",
			db:   &Database{fastRecovery: true, useBase: true, config: &Config{TrieNodeBufferType: NodeBufferList}},
			err:  errMissJournal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.False(t, test.db.shouldRecoverFromStateHistory(test.err))
		})
	}
}
