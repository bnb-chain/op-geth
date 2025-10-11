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
