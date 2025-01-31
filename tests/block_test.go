// Copyright 2015 The go-ethereum Authors
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

package tests

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestBlockchainWithTxDAG(t *testing.T) {
	//log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))
	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	// For speedier CI-runs, the line below can be uncommented, so those are skipped.
	// For now, in hardfork-times (Berlin), we run the tests both as StateTests and
	// as blockchain tests, since the latter also covers things like receipt root
	bt.skipLoad(`^GeneralStateTests/`)

	// Skip random failures due to selfish mining test
	bt.skipLoad(`.*bcForgedTest/bcForkUncle\.json`)

	// Slow tests
	bt.slow(`.*bcExploitTest/DelegateCallSpam.json`)
	bt.slow(`.*bcExploitTest/ShanghaiLove.json`)
	bt.slow(`.*bcExploitTest/SuicideIssue.json`)
	bt.slow(`.*/bcForkStressTest/`)
	bt.slow(`.*/bcGasPricerTest/RPC_API_Test.json`)
	bt.slow(`.*/bcWalletTest/`)

	// Very slow test
	bt.skipLoad(`.*/stTimeConsuming/.*`)
	// test takes a lot for time and goes easily OOM because of sha3 calculation on a huge range,
	// using 4.6 TGas
	bt.skipLoad(`.*randomStatetest94.json.*`)

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		if runtime.GOARCH == "386" && runtime.GOOS == "windows" && rand.Int63()%2 == 0 {
			t.Skip("test (randomly) skipped on 32-bit windows")
		}
		execBlockTestWithTxDAG(t, bt, test, name)
	})
	//bt := new(testMatcher)
	//path := filepath.Join(blockTestDir, "ValidBlocks", "bcEIP1559", "intrinsic.json")
	//_, name := filepath.Split(path)
	//t.Run(name, func(t *testing.T) {
	//	bt.runTestFile(t, path, name, func(t *testing.T, name string, test *BlockTest) {
	//		if runtime.GOARCH == "386" && runtime.GOOS == "windows" && rand.Int63()%2 == 0 {
	//			t.Skip("test (randomly) skipped on 32-bit windows")
	//		}
	//		execBlockTestWithTxDAG(t, bt, test)
	//	})
	//})
}

func TestBlockchain(t *testing.T) {
	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	// For speedier CI-runs, the line below can be uncommented, so those are skipped.
	// For now, in hardfork-times (Berlin), we run the tests both as StateTests and
	// as blockchain tests, since the latter also covers things like receipt root
	bt.skipLoad(`^GeneralStateTests/`)

	// Skip random failures due to selfish mining test
	bt.skipLoad(`.*bcForgedTest/bcForkUncle\.json`)

	// Slow tests
	bt.slow(`.*bcExploitTest/DelegateCallSpam.json`)
	bt.slow(`.*bcExploitTest/ShanghaiLove.json`)
	bt.slow(`.*bcExploitTest/SuicideIssue.json`)
	bt.slow(`.*/bcForkStressTest/`)
	bt.slow(`.*/bcGasPricerTest/RPC_API_Test.json`)
	bt.slow(`.*/bcWalletTest/`)

	// Very slow test
	bt.skipLoad(`.*/stTimeConsuming/.*`)
	// test takes a lot for time and goes easily OOM because of sha3 calculation on a huge range,
	// using 4.6 TGas
	bt.skipLoad(`.*randomStatetest94.json.*`)

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		if runtime.GOARCH == "386" && runtime.GOOS == "windows" && rand.Int63()%2 == 0 {
			t.Skip("test (randomly) skipped on 32-bit windows")
		}
		execBlockTest(t, bt, test)
	})
	// There is also a LegacyTests folder, containing blockchain tests generated
	// prior to Istanbul. However, they are all derived from GeneralStateTests,
	// which run natively, so there's no reason to run them here.
}

// TestExecutionSpecBlocktests runs the test fixtures from execution-spec-tests.
func TestExecutionSpecBlocktests(t *testing.T) {
	if !common.FileExist(executionSpecBlockchainTestDir) {
		t.Skipf("directory %s does not exist", executionSpecBlockchainTestDir)
	}
	bt := new(testMatcher)

	bt.walk(t, executionSpecBlockchainTestDir, func(t *testing.T, name string, test *BlockTest) {
		execBlockTest(t, bt, test)
	})
}

var txDAGFileCounter atomic.Uint64

func execBlockTestWithTxDAG(t *testing.T, bt *testMatcher, test *BlockTest, name string) {
	txDAGFile := "../tests-dag/dag/" + name
	if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, false, nil, "", false, false, false)); err != nil {
		t.Errorf("test in path mode with snapshotter failed: %v", err)
		return
	}

	// run again with dagFile
	//core.InitPevmRunner(2)
	if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, false, nil, txDAGFile, true, false, false)); err != nil {
		t.Errorf("test in path mode with snapshotter failed: %v", err)
		return
	}

	if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, false, nil, txDAGFile, true, true, false)); err != nil {
		t.Errorf("test in path mode with snapshotter failed: %v", err)
		return
	}

	if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, false, nil, txDAGFile, true, false, true)); err != nil {
		t.Errorf("test in path mode with snapshotter failed: %v", err)
		return
	}
}

func execBlockTest(t *testing.T, bt *testMatcher, test *BlockTest) {
	if err := bt.checkFailure(t, test.Run(false, rawdb.HashScheme, nil, false, nil, "", true, false, false)); err != nil {
		t.Errorf("test in hash mode without snapshotter failed: %v", err)
		return
	}
	if err := bt.checkFailure(t, test.Run(true, rawdb.HashScheme, nil, false, nil, "", true, false, false)); err != nil {
		t.Errorf("test in hash mode with snapshotter failed: %v", err)
		return
	}
	if err := bt.checkFailure(t, test.Run(false, rawdb.PathScheme, nil, false, nil, "", true, false, false)); err != nil {
		t.Errorf("test in path mode without snapshotter failed: %v", err)
		return
	}
	if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, false, nil, "", true, false, false)); err != nil {
		t.Errorf("test in path mode with snapshotter failed: %v", err)
		return
	}

}

func TestBlockchainWithTxDAGGen(t *testing.T) {
	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	// For speedier CI-runs, the line below can be uncommented, so those are skipped.
	// For now, in hardfork-times (Berlin), we run the tests both as StateTests and
	// as blockchain tests, since the latter also covers things like receipt root
	bt.skipLoad(`^GeneralStateTests/`)

	// Skip random failures due to selfish mining test
	bt.skipLoad(`.*bcForgedTest/bcForkUncle\.json`)

	// Slow tests
	bt.slow(`.*bcExploitTest/DelegateCallSpam.json`)
	bt.slow(`.*bcExploitTest/ShanghaiLove.json`)
	bt.slow(`.*bcExploitTest/SuicideIssue.json`)
	bt.slow(`.*/bcForkStressTest/`)
	bt.slow(`.*/bcGasPricerTest/RPC_API_Test.json`)
	bt.slow(`.*/bcWalletTest/`)

	// Very slow test
	bt.skipLoad(`.*/stTimeConsuming/.*`)
	// test takes a lot for time and goes easily OOM because of sha3 calculation on a huge range,
	// using 4.6 TGas
	bt.skipLoad(`.*randomStatetest94.json.*`)

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		if runtime.GOARCH == "386" && runtime.GOOS == "windows" && rand.Int63()%2 == 0 {
			t.Skip("test (randomly) skipped on 32-bit windows")
		}
		if err := bt.checkFailure(t, test.Run(true, rawdb.PathScheme, nil, true, nil, "", false, false, false)); err != nil {
			t.Errorf("test in path mode with snapshotter failed: %v", err)
			return
		}
	})
}
