package pathdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/trie/testutil"
	"github.com/stretchr/testify/assert"
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

func TestCalculateNodeBufferListInfo(t *testing.T) {
	cases := []struct {
		name                 string
		proposeBlockInterval uint64
		proofTimeInHours     uint64
		wantedResult1        uint64
		wantedResult2        uint64
		wantedResult3        uint64
	}{
		{
			name:                 "both proposeBlockInterval and proofTimeInHours are 0",
			proposeBlockInterval: 0,
			proofTimeInHours:     0,
			wantedResult1:        3600,
			wantedResult2:        3,
			wantedResult3:        1800,
		},
		{
			name:                 "proofTimeInHours is 0, proposeBlockInterval is divided by 2",
			proposeBlockInterval: 3600,
			proofTimeInHours:     0,
			wantedResult1:        3600,
			wantedResult2:        3,
			wantedResult3:        1800,
		},
		{
			name:                 "proofTimeInHours is 0, proposeBlockInterval cannot be divided by 2",
			proposeBlockInterval: 1931,
			proofTimeInHours:     0,
			wantedResult1:        1931,
			wantedResult2:        1,
			wantedResult3:        1931,
		},
		{
			name:                 "proposeBlockInterval is 0, proofTimeInHours is 1",
			proposeBlockInterval: 0,
			proofTimeInHours:     1,
			wantedResult1:        3600,
			wantedResult2:        2,
			wantedResult3:        3600,
		},
		{
			name:                 "proposeBlockInterval is 0, proofTimeInHours is 2",
			proposeBlockInterval: 0,
			proofTimeInHours:     2,
			wantedResult1:        3600,
			wantedResult2:        3,
			wantedResult3:        3600,
		},
		{
			name:                 "proposeBlockInterval is 240, proofTimeInHours is 1",
			proposeBlockInterval: 240,
			proofTimeInHours:     1,
			wantedResult1:        240,
			wantedResult2:        2,
			wantedResult3:        240,
		},
		{
			name:                 "proposeBlockInterval is 240, proofTimeInHours is 2",
			proposeBlockInterval: 240,
			proofTimeInHours:     2,
			wantedResult1:        240,
			wantedResult2:        3,
			wantedResult3:        240,
		},
		{
			name:                 "proposeBlockInterval is 1931, proofTimeInHours is 2",
			proposeBlockInterval: 1931,
			proofTimeInHours:     2,
			wantedResult1:        1931,
			wantedResult2:        1,
			wantedResult3:        1931,
		},
	}
	for _, tt := range cases {
		t.Run("Test calculateNodeBufferListInfo", func(t *testing.T) {
			wpBlocks, rsevMdNum, dlInMd := calculateNodeBufferListInfo(tt.proposeBlockInterval, tt.proofTimeInHours)

			if wpBlocks != tt.wantedResult1 {
				t.Errorf("wanted wpBlocks: %d, got wpBlocks: %d", tt.wantedResult1, wpBlocks)
			}
			if rsevMdNum != tt.wantedResult2 {
				t.Errorf("wanted rsevMdNum: %d, got rsevMdNum: %d", tt.wantedResult2, rsevMdNum)
			}
			if dlInMd != tt.wantedResult3 {
				t.Errorf("wanted dlInMd: %d, got dlInMd: %d", tt.wantedResult3, dlInMd)
			}
		})
	}
}
