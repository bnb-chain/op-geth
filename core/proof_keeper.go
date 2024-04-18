package core

import (
	"context"
	"encoding/json"
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

// keeperMetaRecord is used to ensure proof continuous in scenarios such as enable/disable keeper, interval changes, reorg, etc.
// which is stored in kv db.
type keeperMetaRecord struct {
	blockID          uint64
	proofID          uint64
	proposedInterval uint64
}

// proofDataRecord is used to store proposed proof data.
// which is stored in ancient db.
type proofDataRecord struct {
	ProofID      uint64         `json:"proofID"`
	BlockID      uint64         `json:"blockID"`
	StateRoot    common.Hash    `json:"stateRoot"`
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
		BlockID:      kRecord.BlockID,
		StateRoot:    kRecord.StateRoot,
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
	var (
		putKeeperMetaRecordOnce bool
	)
	for {
		select {
		case r := <-keeper.opts.watchStartKeepCh:
			var (
				hasTruncatedMeta bool
				curProofID       uint64
			)
			hasTruncatedMeta = keeper.truncateKeeperMetaRecordHeadIfNeeded(r.BlockID)
			metaList := keeper.getKeeperMetaRecordList()
			if len(metaList) == 0 {
				keeper.proofDataDB.Reset()
				curProofID = 1
			} else {
				keeper.truncateProofDataRecordHeadIfNeeded(r.BlockID)
				latestProofData := keeper.getLatestProofDataRecord()
				if latestProofData != nil {
					curProofID = latestProofData.ProofID + 1
				} else {
					curProofID = 1
				}
			}

			log.Info("keep proof", "record", r)
			proofRecord, err := keeper.queryProposedProof(r)
			if err == nil {
				if hasTruncatedMeta || !putKeeperMetaRecordOnce {
					putKeeperMetaRecordOnce = true
					keeper.putKeeperMetaRecord(&keeperMetaRecord{
						proposedInterval: keeper.opts.keepInterval,
						blockID:          r.BlockID,
						proofID:          curProofID,
					})
				}
				proofRecord.ProofID = curProofID
				keeper.putProofDataRecord(proofRecord)
			}
			keeper.opts.notifyFinishKeepCh <- struct{}{}
		}
	}
}

// inner util func list
// keeper meta func
func (keeper *proofKeeper) getKeeperMetaRecordList() []keeperMetaRecord {
	var (
		metaList []keeperMetaRecord
		err      error
		iter     ethdb.Iterator
	)
	iter = rawdb.IterateKeeperMeta(keeper.keeperMetaDB)
	defer iter.Release()

	for iter.Next() {
		m := keeperMetaRecord{}
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			continue
		}
		metaList = append(metaList, m)
	}
	return metaList
}

func (keeper *proofKeeper) truncateKeeperMetaRecordHeadIfNeeded(blockID uint64) bool {
	var (
		err          error
		iter         ethdb.Iterator
		batch        ethdb.Batch
		hasTruncated bool
	)
	iter = rawdb.IterateKeeperMeta(keeper.keeperMetaDB)
	defer iter.Release()

	batch = keeper.keeperMetaDB.NewBatch()

	for iter.Next() {
		m := keeperMetaRecord{}
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			continue
		}
		if m.blockID >= blockID {
			hasTruncated = true
			rawdb.DeleteKeeperMeta(batch, m.blockID)
		}

	}
	err = batch.Write()
	if err != nil {
		log.Crit("Failed to truncate keeper meta head", "err", err)
	}
	return hasTruncated
}

func (keeper *proofKeeper) putKeeperMetaRecord(m *keeperMetaRecord) {
	meta, err := json.Marshal(*m)
	if err != nil {
		log.Crit("Failed to marshal keeper meta record", "err", err)
	}
	rawdb.PutKeeperMeta(keeper.keeperMetaDB, m.blockID, meta)
	log.Info("Succeed to add keeper meta", "record", m)

}

// proof data func
func (keeper *proofKeeper) truncateProofDataRecordHeadIfNeeded(blockID uint64) {
	latestProofDataRecord := keeper.getLatestProofDataRecord()
	if latestProofDataRecord == nil {
		return
	}
	if blockID > latestProofDataRecord.BlockID {
		return
	}

	truncateProofID := uint64(0)
	proofID := latestProofDataRecord.ProofID
	for proofID > 0 {
		proof := keeper.getProofDataRecord(proofID)
		if proof == nil {
			keeper.proofDataDB.Reset()
			return
		}
		if proof.BlockID < blockID {
			truncateProofID = proof.ProofID
			break
		}
		proofID = proofID - 1
	}
	rawdb.TruncateProofDataHead(keeper.proofDataDB, truncateProofID)
}

func (keeper *proofKeeper) getLatestProofDataRecord() *proofDataRecord {
	latestProofData := rawdb.GetLatestProofData(keeper.proofDataDB)
	if latestProofData == nil {
		return nil
	}
	var data proofDataRecord
	err := json.Unmarshal(latestProofData, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	return &data
}

func (keeper *proofKeeper) getProofDataRecord(proofID uint64) *proofDataRecord {
	latestProofData := rawdb.GetProofData(keeper.proofDataDB, proofID)
	if latestProofData == nil {
		return nil
	}
	var data proofDataRecord
	err := json.Unmarshal(latestProofData, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	return &data
}

func (keeper *proofKeeper) putProofDataRecord(p *proofDataRecord) {
	proof, err := json.Marshal(*p)
	if err != nil {
		log.Crit("Failed to marshal proof data", "err", err)
	}
	rawdb.PutProofData(keeper.proofDataDB, p.ProofID, proof)
	log.Info("Succeed to add proof data", "record", p)
}
