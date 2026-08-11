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
