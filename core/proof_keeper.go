package core

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie/triedb/pathdb"
)

const (
	l2ToL1MessagePasser = "0x4200000000000000000000000000000000000016"
)

var (
	l2ToL1MessagePasserAddr = common.HexToAddress(l2ToL1MessagePasser)
)

type proofDataRecord struct {
	Address      common.Address `json:"address"`
	AccountProof []string       `json:"accountProof"`
	Balance      *big.Int       `json:"balance"`
	CodeHash     common.Hash    `json:"codeHash"`
	Nonce        uint64         `json:"nonce"`
	StorageHash  common.Hash    `json:"storageHash"`
}

type proofKeeperOptions struct {
	enable             bool
	keepInterval       uint64
	watchStartKeepCh   chan *pathdb.KeepRecord
	notifyFinishKeepCh chan struct{}
	rpcClient          *rpc.Client
}

type proofKeeper struct {
	opts         *proofKeeperOptions
	keeperMetaDB ethdb.Database
	proofDataDB  *rawdb.ResettableFreezer
	selfClient   *gethclient.Client
}

func newProofKeeper(keeperMetaDB ethdb.Database, opts *proofKeeperOptions) (*proofKeeper, error) {
	var (
		err        error
		ancientDir string
		keeper     *proofKeeper
	)

	if ancientDir, err = keeperMetaDB.AncientDatadir(); err != nil {
		log.Error("Failed to get ancient data dir", "error", err)
		return nil, err
	}
	keeper = &proofKeeper{
		opts:         opts,
		keeperMetaDB: keeperMetaDB,
		selfClient:   gethclient.New(opts.rpcClient),
	}
	if keeper.proofDataDB, err = rawdb.NewProofFreezer(ancientDir, false); err != nil {
		log.Error("Failed to new proof ancient freezer", "error", err)
		return nil, err
	}

	go keeper.eventLoop()

	log.Info("Succeed to init proof keeper", "options", opts)
	return keeper, nil
}

func (keeper *proofKeeper) queryProposedProof(kRecord *pathdb.KeepRecord) (*proofDataRecord, error) {
	rawPoof, err := keeper.selfClient.GetProof(context.Background(), l2ToL1MessagePasserAddr, nil, big.NewInt(int64(kRecord.BlockID)))

	if err != nil {
		log.Error("Failed to get proof",
			"block_id", kRecord.BlockID,
			"hex_block_id_1", hexutil.EncodeUint64(kRecord.BlockID),
			"hex_block_id_2", hexutil.Uint64(kRecord.BlockID).String(),
			"error", err)
		return nil, err
	}
	pRecord := &proofDataRecord{
		Address:      rawPoof.Address,
		AccountProof: rawPoof.AccountProof,
		Balance:      rawPoof.Balance,
		CodeHash:     rawPoof.CodeHash,
		Nonce:        rawPoof.Nonce,
		StorageHash:  rawPoof.StorageHash,
	}
	log.Info("Succeed to get proof", "proof_record", pRecord)
	return pRecord, nil
}

func (keeper *proofKeeper) eventLoop() {
	for {
		select {
		case r := <-keeper.opts.watchStartKeepCh:
			log.Info("keep proof", "record", r)
			keeper.queryProposedProof(r)
			keeper.opts.notifyFinishKeepCh <- struct{}{}
		}
	}
}
