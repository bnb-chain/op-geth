package core

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
)

type parallel chan func()

func (p parallel) do(f func()) {
	p <- f
}

func (p parallel) close() {
	close(p)
}

func (p parallel) start(pnum int) {
	for i := 0; i < pnum; i++ {
		go func() {
			for f := range p {
				f()
			}
		}()
	}
}

type keypair struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

func randAddress() (common.Address, *ecdsa.PrivateKey) {
	// Generate a new private key using rand.Reader
	key, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	return crypto.PubkeyToAddress(key.PublicKey), key
}

func generateAddress(num int) []*keypair {
	proc := parallel(make(chan func()))
	proc.start(16)
	address := make([]*keypair, num)
	wait := sync.WaitGroup{}
	wait.Add(num)
	for i := 0; i < num; i++ {
		index := i
		proc.do(func() {
			addr, key := randAddress()
			address[index] = &keypair{key, addr}
			wait.Done()
		})
	}
	wait.Wait()
	proc.close()
	return address
}

func genesisAlloc(addresses []*keypair, funds *big.Int) GenesisAlloc {
	alloc := GenesisAlloc{}
	for _, addr := range addresses {
		alloc[addr.addr] = GenesisAccount{Balance: funds}
	}
	return alloc
}

func TestPevmInsertChain(t *testing.T) {
	//from := 8000
	//to := 8001
	//endpoint := "https://opbnb-qanet-ec-5-seq-pevm-2.bk.nodereal.cc"
	//gJson := "/Users/awen/Desktop/Nodereal/aweneagle_projects/chain-infra/qa/gitops/qa-us/opbnb-qanet-ec-5/contracts-info/genesis.json"
	//genesis := loadGenesis(gJson)
	//blocks := fetchBlocks(uint64(from), uint64(to), endpoint)
	////preapare parent header
	//chain := buildBlockChain(genesis, false)
	//chain.hc.headerCache.Add(blocks[0].Hash(), blocks[0].Header())
	//if err := InsertChain(chain, blocks[1:]); err != nil {
	//	t.Fatal(err)
	//}
}

func fetchBlocks(from, to uint64, endpoint string) []*types.Block {
	client, err := ethclient.Dial(endpoint)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var blocks []*types.Block
	for i := from; i <= to; i++ {
		block, err := client.BlockByNumber(context.Background(), big.NewInt(int64(i)))
		if err != nil {
			panic(err)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

func loadGenesis(jsonfile string) *Genesis {
	// Open the JSON file
	file, err := os.Open(jsonfile)
	if err != nil {
		panic("failed to open json file, err=" + err.Error())
	}
	defer file.Close()

	// Read the file content
	bytes, err := io.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("failed to read genesis file: %v", err))
	}

	// Unmarshal the JSON content into the Genesis struct
	var genesis Genesis
	if err := json.Unmarshal(bytes, &genesis); err != nil {
		panic(fmt.Sprintf("failed to unmarshal genesis JSON: %v", err))
	}
	return &genesis
}

func buildBlockChain(genesis *Genesis, parallel bool) *BlockChain {
	archiveDb := rawdb.NewMemoryDatabase()
	// Import the chain as an archive node for the comparison baseline
	archive, err := NewBlockChain(archiveDb, DefaultCacheConfigWithScheme(rawdb.PathScheme), genesis, nil, ethash.NewFaker(), vm.Config{EnableParallelExec: parallel}, nil, nil)
	if err != nil {
		panic(err)
	}
	return archive
}

func InsertChain(bc *BlockChain, blocks []*types.Block) error {
	_, err := bc.InsertChain(blocks)
	return err
}

func BenchmarkAkaka(b *testing.B) {
	addrNum := 10000
	// Configure and generate a sample block chain
	funds := big.NewInt(1000000000000000)
	addresses := generateAddress(addrNum)
	genesisAlloc := genesisAlloc(addresses, funds)
	randomAddr := make(chan common.Address, addrNum)
	for addr := range genesisAlloc {
		randomAddr <- addr
	}
	var (
		gspec = &Genesis{
			Config:   params.TestChainConfig,
			Alloc:    genesisAlloc,
			BaseFee:  big.NewInt(params.InitialBaseFee),
			GasLimit: 500000000,
		}
		signer = types.LatestSigner(gspec.Config)
	)

	_, blocks, _ := GenerateChainWithGenesis(gspec, ethash.NewFaker(), 2, func(i int, block *BlockGen) {
		block.SetCoinbase(common.Address{0x00})
		txs := make([]*types.Transaction, len(addresses))
		for i, addr := range addresses {
			// borrow an address
			to := <-randomAddr
			from := addr
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(from.addr), to, big.NewInt(1000), params.TxGas, block.header.BaseFee, nil), signer, from.key)
			if err != nil {
				panic(err)
			}
			txs[i] = tx
			randomAddr <- to
		}
		for i := 0; i < len(txs); i++ {
			block.AddTx(txs[i])
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		archiveDb := rawdb.NewMemoryDatabase()
		// Import the chain as an archive node for the comparison baseline
		archive, _ := NewBlockChain(archiveDb, DefaultCacheConfigWithScheme(rawdb.PathScheme), gspec, nil, ethash.NewFaker(), vm.Config{EnableParallelExec: true}, nil, nil)
		if n, err := archive.InsertChain(blocks); err != nil {
			panic(fmt.Sprintf("failed to process block %d: %v", n, err))
		}
		archive.Stop()
	}
}

var cacheLock = sync.RWMutex{}
var cached = make(map[common.Address]uint64)

func getAddressSafe(addr common.Address) uint64 {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	return cached[addr]
}

func getAddress(addr common.Address) uint64 {
	return cached[addr]
}

func BenchmarkSequencialRead(b *testing.B) {
	// generate a set of addresses
	// case 1: read the state sequentially
	// case 2: read them in parallel
	address := generateAddress(10000)
	for i := 0; i < len(address); i++ {
		cached[address[i].addr] = uint64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// iterate all the address
		for i := 0; i < len(address); i++ {
			getAddress(address[i].addr)
		}
	}
}

func BenchmarkParallelRead(b *testing.B) {
	// generate a set of addresses
	// case 1: read the state sequentially
	// case 2: read them in parallel
	address := generateAddress(10000)
	for i := 0; i < len(address); i++ {
		cached[address[i].addr] = uint64(i)
	}

	proc := parallel(make(chan func()))
	proc.start(runtime.NumCPU())
	wait := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// iterator all the address in parallel
		wait.Add(len(address))
		for i := 0; i < len(address); i++ {
			index := i
			proc.do(func() {
				getAddressSafe(address[index].addr)
				wait.Done()
			})
		}
		wait.Wait()
	}
	proc.close()
}
