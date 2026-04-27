// verifyproof is a standalone tool that verifies the correctness of
// account proofs returned by the proof keeper for the L2ToL1MessagePasser
// pre-deploy. It picks several "snapshot" blocks (those divisible by 3600,
// matching the default keeper interval), fetches both the block header
// (for the trusted state root) and the eth_getProof result, and then runs
// a full Merkle Patricia Trie verification locally.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// l2ToL1MessagePasser is the OP Stack pre-deploy whose proof the
	// keeper persists for op-proposer / withdrawal proving.
	l2ToL1MessagePasser = "0x4200000000000000000000000000000000000016"

	// keeperInterval matches gcProofIntervalSecond / KeepInterval default;
	// proofs are only retained for blocks where blockID % interval == 0.
	keeperInterval uint64 = 3600

	// historyCount is how many sampled blocks to verify, including the
	// latest sampled one. Tweak via -count.
	defaultHistoryCount = 20
)

var (
	rpcURL    = flag.String("rpc", "http://localhost:8545", "L2 JSON-RPC endpoint")
	count     = flag.Int("count", defaultHistoryCount, "number of sampled blocks to verify")
	interval  = flag.Uint64("interval", keeperInterval, "keeper sampling interval (must match node config)")
	address   = flag.String("address", l2ToL1MessagePasser, "account address to prove")
	timeoutMs = flag.Int("timeout-ms", 15000, "per-RPC timeout in milliseconds")
	verbose   = flag.Bool("v", false, "verbose: print proof structure on success")

	// Storage-level verification: automatically fetch MessagePassed events
	// from each interval and verify their storage proofs, modelling what
	// OptimismPortal.proveWithdrawalTransaction does on L1.
	fetchWithdrawals = flag.Bool("fetch-withdrawals", false,
		"fetch MessagePassed events and verify sentMessages storage proofs")
	sentMessagesSlot = flag.Uint64("slot", 0,
		"storage slot index of the sentMessages mapping (default 0, OP Stack pre-deploy)")

	// Dump mode: fetch proof data and save to JSON file.
	dumpFile   = flag.String("dump", "", "dump proof data to JSON file instead of verifying")
	dumpBlocks = flag.Uint64("dump-blocks", 86400, "block range to scan when dumping (latest N blocks)")
)

// jsonRPCRequest / jsonRPCResponse are the minimal envelope structs used
// to talk to the JSON-RPC endpoint. We avoid pulling in ethclient so the
// tool can also speak to non-Geth backends and stays standalone.
type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      uint64 `json:"id"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error"`
}

// accountProofResult mirrors internal/ethapi.AccountResult.
type accountProofResult struct {
	Address      common.Address  `json:"address"`
	AccountProof []string        `json:"accountProof"`
	Balance      *hexutil.Big    `json:"balance"`
	CodeHash     common.Hash     `json:"codeHash"`
	Nonce        hexutil.Uint64  `json:"nonce"`
	StorageHash  common.Hash     `json:"storageHash"`
	StorageProof []storageResult `json:"storageProof"`
}

type storageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

// header is a tiny shape that only extracts the state root we care about.
type header struct {
	Number    hexutil.Big `json:"number"`
	StateRoot common.Hash `json:"stateRoot"`
	Hash      common.Hash `json:"hash"`
}

// messagePassedTopic is keccak256("MessagePassed(uint256,address,address,uint256,uint256,bytes,bytes32)").
// Used to filter event logs from the L2ToL1MessagePasser contract.
var messagePassedTopic = crypto.Keccak256Hash([]byte(
	"MessagePassed(uint256,address,address,uint256,uint256,bytes,bytes32)"))

// logEntry represents a single Ethereum event log returned by eth_getLogs.
type logEntry struct {
	Address     common.Address `json:"address"`
	Topics      []common.Hash  `json:"topics"`
	Data        hexutil.Bytes  `json:"data"`
	BlockNumber string         `json:"blockNumber"`
	TxHash      common.Hash    `json:"transactionHash"`
}

// blockProofDump is the JSON structure for one block in the dump output.
type blockProofDump struct {
	BlockNumber uint64              `json:"blockNumber"`
	BlockHash   common.Hash         `json:"blockHash"`
	StateRoot   common.Hash         `json:"stateRoot"`
	Proof       *accountProofResult `json:"proof"`
}

// memProofDB satisfies ethdb.KeyValueReader for trie.VerifyProof.
// It only stores keccak256(rlp(node)) -> rlp(node) entries.
type memProofDB struct {
	kv map[string][]byte
}

func newMemProofDB() *memProofDB { return &memProofDB{kv: make(map[string][]byte)} }

func (m *memProofDB) Has(key []byte) (bool, error) {
	_, ok := m.kv[string(key)]
	return ok, nil
}

func (m *memProofDB) Get(key []byte) ([]byte, error) {
	v, ok := m.kv[string(key)]
	if !ok {
		return nil, errors.New("not found")
	}
	return v, nil
}

func (m *memProofDB) put(key, value []byte) { m.kv[string(key)] = value }

// idGen produces incrementing JSON-RPC request IDs.
var idGen atomic.Uint64

func main() {
	flag.Parse()
	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "count must be > 0")
		os.Exit(2)
	}
	if *interval == 0 {
		fmt.Fprintln(os.Stderr, "interval must be > 0")
		os.Exit(2)
	}
	if !common.IsHexAddress(*address) {
		fmt.Fprintln(os.Stderr, "invalid address:", *address)
		os.Exit(2)
	}
	addr := common.HexToAddress(*address)

	if *fetchWithdrawals {
		fmt.Printf("withdrawal mode: sentMessages slot=%d\n", *sentMessagesSlot)
	}

	ctx := context.Background()

	latest, err := getLatestBlockNumber(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "eth_blockNumber failed:", err)
		os.Exit(1)
	}
	fmt.Printf("latest block: %d\n", latest)

	// Dump mode: fetch proofs and write to JSON file, then exit.
	if *dumpFile != "" {
		if err := dumpProofs(ctx, addr, latest, *dumpBlocks, *interval, *dumpFile); err != nil {
			fmt.Fprintln(os.Stderr, "dump failed:", err)
			os.Exit(1)
		}
		return
	}

	// Largest block divisible by interval that is <= latest.
	if latest < *interval {
		fmt.Fprintf(os.Stderr, "latest block %d is below sampling interval %d, nothing to verify\n", latest, *interval)
		os.Exit(1)
	}
	top := (latest / *interval) * *interval
	blocks := make([]uint64, 0, *count)
	for i := 0; i < *count; i++ {
		bn := top - uint64(i)**interval
		if bn == 0 {
			break
		}
		blocks = append(blocks, bn)
	}
	fmt.Printf("verifying %d sampled blocks from %d down to %d (step=%d)\n",
		len(blocks), blocks[0], blocks[len(blocks)-1], *interval)

	var (
		ok   int
		fail int
	)
	for _, bn := range blocks {
		var (
			sk []common.Hash
			wh []common.Hash
		)

		// When -fetch-withdrawals is set, query MessagePassed events for
		// the interval ending at this snapshot block.
		if *fetchWithdrawals {
			fromBN := bn - *interval + 1
			if fromBN == 0 {
				fromBN = 1
			}
			fetched, fetchErr := fetchWithdrawalHashesFromLogs(ctx, addr, fromBN, bn)
			if fetchErr != nil {
				fail++
				fmt.Printf("[FAIL] block=%-10d err=fetch logs: %v\n", bn, fetchErr)
				continue
			}
			wh = fetched
			sk = computeStorageKeys(wh, *sentMessagesSlot)
			if len(wh) > 0 {
				fmt.Printf("       block=%-10d fetched %d withdrawal(s) from [%d, %d]\n",
					bn, len(wh), fromBN, bn)
			}
		}

		if err := verifyOne(ctx, addr, bn, sk, wh); err != nil {
			fail++
			fmt.Printf("[FAIL] block=%-10d err=%v\n", bn, err)
			continue
		}
		ok++
		fmt.Printf("[ OK ] block=%-10d\n", bn)
	}
	fmt.Printf("\nresult: %d ok, %d fail\n", ok, fail)
	if fail > 0 {
		os.Exit(1)
	}
}

func verifyOne(ctx context.Context, addr common.Address, blockNum uint64,
	storageKeys []common.Hash, withdrawalHashList []common.Hash) error {
	hexBN := hexutil.EncodeUint64(blockNum)

	hdr, err := getHeader(ctx, hexBN)
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber: %w", err)
	}
	if hdr == nil {
		return fmt.Errorf("block %d not found", blockNum)
	}

	res, err := getProof(ctx, addr, storageKeys, hexBN)
	if err != nil {
		return fmt.Errorf("eth_getProof: %w", err)
	}
	if res.Address != addr {
		return fmt.Errorf("address mismatch: got %s want %s", res.Address.Hex(), addr.Hex())
	}
	if len(res.AccountProof) == 0 {
		return errors.New("empty account proof")
	}

	// Build an in-memory hash -> node KV from the proof list.
	db := newMemProofDB()
	for i, hexNode := range res.AccountProof {
		nodeBytes, err := hexutil.Decode(hexNode)
		if err != nil {
			return fmt.Errorf("decode proof[%d]: %w", i, err)
		}
		db.put(crypto.Keccak256(nodeBytes), nodeBytes)
	}

	// Walk the proof from stateRoot using keccak256(address) as the key.
	key := crypto.Keccak256(addr.Bytes())
	leaf, err := trie.VerifyProof(hdr.StateRoot, key, db)
	if err != nil {
		return fmt.Errorf("trie.VerifyProof: %w", err)
	}

	// A nil leaf means the account does not exist at that state. For the
	// L2ToL1MessagePasser pre-deploy this should never happen; treat as fail.
	if leaf == nil {
		return errors.New("account not found in trie (nil leaf)")
	}

	// Decode the leaf and compare against the returned account fields.
	var acc types.StateAccount
	if err := rlp.DecodeBytes(leaf, &acc); err != nil {
		return fmt.Errorf("decode state account: %w", err)
	}
	wantBalance := (*big.Int)(res.Balance)
	if wantBalance == nil {
		wantBalance = new(big.Int)
	}
	gotBalance := new(big.Int)
	if acc.Balance != nil {
		gotBalance = acc.Balance.ToBig()
	}
	if gotBalance.Cmp(wantBalance) != 0 {
		return fmt.Errorf("balance mismatch: trie=%s rpc=%s", gotBalance, wantBalance)
	}
	if acc.Nonce != uint64(res.Nonce) {
		return fmt.Errorf("nonce mismatch: trie=%d rpc=%d", acc.Nonce, uint64(res.Nonce))
	}
	if acc.Root != res.StorageHash {
		return fmt.Errorf("storage root mismatch: trie=%s rpc=%s", acc.Root.Hex(), res.StorageHash.Hex())
	}
	if !bytes.Equal(acc.CodeHash, res.CodeHash[:]) {
		return fmt.Errorf("code hash mismatch: trie=%x rpc=%s", acc.CodeHash, res.CodeHash.Hex())
	}

	if *verbose {
		fmt.Printf("       stateRoot=%s\n", hdr.StateRoot.Hex())
		fmt.Printf("       proofNodes=%d nonce=%d balance=%s storageHash=%s\n",
			len(res.AccountProof), acc.Nonce, gotBalance, acc.Root.Hex())
	}

	// Optional second-level (storage) proof, mirroring what
	// OptimismPortal.proveWithdrawalTransaction does on L1: prove that
	// `sentMessages[hash] == true` is recorded in the storage trie
	// rooted at acc.Root (== res.StorageHash).
	if len(storageKeys) > 0 {
		if len(res.StorageProof) != len(storageKeys) {
			return fmt.Errorf("storage proof count mismatch: got=%d want=%d",
				len(res.StorageProof), len(storageKeys))
		}
		for i, sp := range res.StorageProof {
			wantKey := storageKeys[i]
			gotKey, err := decode32(sp.Key)
			if err != nil {
				return fmt.Errorf("storage[%d] key decode: %w", i, err)
			}
			if gotKey != wantKey {
				return fmt.Errorf("storage[%d] key mismatch: rpc=%s want=%s",
					i, gotKey.Hex(), wantKey.Hex())
			}
			if err := verifyStorageSlot(acc.Root, wantKey, sp, withdrawalHashList[i]); err != nil {
				return fmt.Errorf("storage[%d] (withdrawal=%s): %w",
					i, withdrawalHashList[i].Hex(), err)
			}
			if *verbose {
				fmt.Printf("       sentMessages[%s] = true (proven, %d nodes)\n",
					withdrawalHashList[i].Hex(), len(sp.Proof))
			}
		}
	}
	return nil
}

// verifyStorageSlot performs the second-level Merkle Patricia Trie
// verification: that the given storage key resolves, under storageRoot,
// to a leaf encoding boolean true (RLP(0x01)).
func verifyStorageSlot(storageRoot common.Hash, storageKey common.Hash, sp storageResult, withdrawalHash common.Hash) error {
	db := newMemProofDB()
	for j, hexNode := range sp.Proof {
		nodeBytes, err := hexutil.Decode(hexNode)
		if err != nil {
			return fmt.Errorf("decode proof[%d]: %w", j, err)
		}
		db.put(crypto.Keccak256(nodeBytes), nodeBytes)
	}
	leaf, err := trie.VerifyProof(storageRoot, crypto.Keccak256(storageKey.Bytes()), db)
	if err != nil {
		return fmt.Errorf("trie.VerifyProof: %w", err)
	}
	if leaf == nil {
		return errors.New("slot is empty (sentMessages[hash] != true)")
	}
	// In the storage trie the value is stored as RLP(bigEndianBytes(value)).
	// For a `bool true`, that is RLP(0x01) which equals the single byte 0x01.
	var raw []byte
	if err := rlp.DecodeBytes(leaf, &raw); err != nil {
		// Some trie encodings store the leaf already as the raw value;
		// fall back to comparing directly.
		raw = leaf
	}
	want := (*big.Int)(sp.Value)
	if want == nil {
		want = new(big.Int)
	}
	got := new(big.Int).SetBytes(raw)
	if got.Cmp(want) != 0 {
		return fmt.Errorf("value mismatch: trie=%s rpc=%s", got, want)
	}
	if got.Sign() == 0 {
		return errors.New("sentMessages[hash] == 0 (not sent)")
	}
	if got.Cmp(big.NewInt(1)) != 0 {
		return fmt.Errorf("unexpected slot value: %s (expected 1 / true)", got)
	}
	return nil
}

// fetchWithdrawalHashesFromLogs queries MessagePassed events in [fromBlock, toBlock]
// and extracts the withdrawalHash from each log entry.
func fetchWithdrawalHashesFromLogs(ctx context.Context, contractAddr common.Address, fromBlock, toBlock uint64) ([]common.Hash, error) {
	logs, err := getLogs(ctx, contractAddr, messagePassedTopic,
		hexutil.EncodeUint64(fromBlock), hexutil.EncodeUint64(toBlock))
	if err != nil {
		return nil, err
	}
	hashes := make([]common.Hash, 0, len(logs))
	for _, lg := range logs {
		// Non-indexed ABI layout: value(32) | gasLimit(32) | dataOffset(32) | withdrawalHash(32) | ...
		// withdrawalHash sits at byte offset 96.
		if len(lg.Data) < 128 {
			continue // malformed log, skip
		}
		var h common.Hash
		copy(h[:], lg.Data[96:128])
		hashes = append(hashes, h)
	}
	return hashes, nil
}

// computeStorageKeys derives the storage trie keys for the given withdrawal
// hashes using the Solidity mapping layout: keccak256(abi.encode(hash, slot)).
func computeStorageKeys(hashes []common.Hash, slot uint64) []common.Hash {
	slotPad := common.BigToHash(new(big.Int).SetUint64(slot))
	keys := make([]common.Hash, len(hashes))
	for i, h := range hashes {
		buf := make([]byte, 0, 64)
		buf = append(buf, h.Bytes()...)
		buf = append(buf, slotPad.Bytes()...)
		keys[i] = crypto.Keccak256Hash(buf)
	}
	return keys
}

// dumpProofs fetches proof data for snapshot blocks in the last `rangeBlocks`
// blocks and writes them to a JSON file.
func dumpProofs(ctx context.Context, addr common.Address, latest, rangeBlocks, intv uint64, outFile string) error {
	bottom := uint64(0)
	if latest > rangeBlocks {
		bottom = latest - rangeBlocks
	}
	// Align bottom up to the next interval boundary.
	if bottom%intv != 0 {
		bottom = (bottom/intv + 1) * intv
	}
	top := (latest / intv) * intv
	if top < bottom {
		return fmt.Errorf("no snapshot blocks in range [%d, %d] with interval %d", latest-rangeBlocks, latest, intv)
	}

	var blocks []uint64
	for bn := top; bn >= bottom && bn > 0; bn -= intv {
		blocks = append(blocks, bn)
	}
	fmt.Printf("dump: fetching %d snapshot blocks from %d down to %d\n", len(blocks), blocks[0], blocks[len(blocks)-1])

	results := make([]blockProofDump, 0, len(blocks))
	for i, bn := range blocks {
		hexBN := hexutil.EncodeUint64(bn)
		hdr, err := getHeader(ctx, hexBN)
		if err != nil {
			fmt.Printf("[WARN] block=%d getHeader: %v (skipping)\n", bn, err)
			continue
		}
		if hdr == nil {
			fmt.Printf("[WARN] block=%d not found (skipping)\n", bn)
			continue
		}
		proof, err := getProof(ctx, addr, nil, hexBN)
		if err != nil {
			fmt.Printf("[WARN] block=%d getProof: %v (skipping)\n", bn, err)
			continue
		}
		results = append(results, blockProofDump{
			BlockNumber: bn,
			BlockHash:   hdr.Hash,
			StateRoot:   hdr.StateRoot,
			Proof:       proof,
		})
		fmt.Printf("  [%d/%d] block=%d ok\n", i+1, len(blocks), bn)
	}

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	if err := os.WriteFile(outFile, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	fmt.Printf("dump: wrote %d proofs to %s (%d bytes)\n", len(results), outFile, len(data))
	return nil
}

func decode32(s string) (common.Hash, error) {
	b, err := hexutil.Decode(s)
	if err != nil {
		return common.Hash{}, err
	}
	if len(b) != 32 {
		return common.Hash{}, fmt.Errorf("expected 32 bytes, got %d", len(b))
	}
	var h common.Hash
	copy(h[:], b)
	return h, nil
}

// ---------------------------------------------------------------------------
// minimal JSON-RPC client
// ---------------------------------------------------------------------------

func rpcCall(ctx context.Context, method string, params []any, out any) error {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      idGen.Add(1),
	}
	buf, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	cctx, cancel := context.WithTimeout(ctx, time.Duration(*timeoutMs)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, http.MethodPost, *rpcURL, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return fmt.Errorf("decode rpc response: %w (body=%s)", err, string(body))
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	if out != nil {
		if err := json.Unmarshal(rpcResp.Result, out); err != nil {
			return fmt.Errorf("decode rpc result: %w (raw=%s)", err, string(rpcResp.Result))
		}
	}
	return nil
}

func getLatestBlockNumber(ctx context.Context) (uint64, error) {
	var out hexutil.Uint64
	if err := rpcCall(ctx, "eth_blockNumber", []any{}, &out); err != nil {
		return 0, err
	}
	return uint64(out), nil
}

func getHeader(ctx context.Context, hexBN string) (*header, error) {
	var out *header
	if err := rpcCall(ctx, "eth_getBlockByNumber", []any{hexBN, false}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func getLogs(ctx context.Context, addr common.Address, topic common.Hash, fromBlock, toBlock string) ([]logEntry, error) {
	filter := map[string]any{
		"address":   addr.Hex(),
		"topics":    []any{topic.Hex()},
		"fromBlock": fromBlock,
		"toBlock":   toBlock,
	}
	var out []logEntry
	if err := rpcCall(ctx, "eth_getLogs", []any{filter}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func getProof(ctx context.Context, addr common.Address, storageKeys []common.Hash, hexBN string) (*accountProofResult, error) {
	keysHex := make([]string, len(storageKeys))
	for i, k := range storageKeys {
		keysHex[i] = k.Hex()
	}
	var out accountProofResult
	if err := rpcCall(ctx, "eth_getProof",
		[]any{addr.Hex(), keysHex, hexBN}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
