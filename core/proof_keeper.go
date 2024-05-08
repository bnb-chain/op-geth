package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	trie2 "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/triedb/pathdb"
)

const (
	// l2ToL1MessagePasser pre-deploy address.
	l2ToL1MessagePasser = "0x4200000000000000000000000000000000000016"

	// gcProofIntervalSecond is used to control gc loop interval.
	gcProofIntervalSecond = 3600

	// maxKeeperMetaNumber is used to gc keep meta, trigger gc workflow
	// when meta number > maxKeeperMetaNumber && meta.block_id < latest_block_id - keep_block_span.
	maxKeeperMetaNumber = 100
)

var (
	l2ToL1MessagePasserAddr = common.HexToAddress(l2ToL1MessagePasser)
	addProofTimer           = metrics.NewRegisteredTimer("proofkeeper/addproof/time", nil)
	getInnerProofTimer      = metrics.NewRegisteredTimer("proofkeeper/getinnerproof/time", nil)
	queryProofTimer         = metrics.NewRegisteredTimer("proofkeeper/queryproof/time", nil)
)

// keeperMetaRecord is used to ensure proof continuous in scenarios such as enable/disable keeper, interval changes, reorg, etc.
// which is stored in kv db, indexed by prefix+block-id.
type keeperMetaRecord struct {
	BlockID      uint64 `json:"blockID"`
	ProofID      uint64 `json:"proofID"`
	KeepInterval uint64 `json:"keepInterval"`
}

// proofDataRecord is used to store proposed proof data.
// which is stored in ancient db, indexed by proof-id.
type proofDataRecord struct {
	ProofID   uint64      `json:"proofID"`
	BlockID   uint64      `json:"blockID"`
	StateRoot common.Hash `json:"stateRoot"`

	Address      common.Address         `json:"address"`
	AccountProof []string               `json:"accountProof"`
	Balance      *big.Int               `json:"balance"`
	CodeHash     common.Hash            `json:"codeHash"`
	Nonce        uint64                 `json:"nonce"`
	StorageHash  common.Hash            `json:"storageHash"`
	StorageProof []common.StorageResult `json:"storageProof"`
}

// proofKeeperOptions defines proof keeper options.
type proofKeeperOptions struct {
	enable             bool
	keepProofBlockSpan uint64
	gcInterval         uint64
	watchStartKeepCh   chan *pathdb.KeepRecord
	notifyFinishKeepCh chan struct{}
}

// ProofKeeper is used to store proposed proof and op-proposer can query.
type ProofKeeper struct {
	opts         *proofKeeperOptions
	blockChain   *BlockChain
	keeperMetaDB ethdb.Database
	proofDataDB  *rawdb.ResettableFreezer

	queryProofCh     chan uint64
	waitQueryProofCh chan *proofDataRecord
	stopCh           chan struct{}
	waitStopCh       chan error
	latestBlockID    uint64
}

// newProofKeeper returns a proof keeper instance.
func newProofKeeper(opts *proofKeeperOptions) *ProofKeeper {
	if opts.keepProofBlockSpan == 0 {
		opts.keepProofBlockSpan = params.FullImmutabilityThreshold
	}
	if opts.gcInterval == 0 {
		opts.gcInterval = gcProofIntervalSecond
	}
	keeper := &ProofKeeper{
		opts:             opts,
		queryProofCh:     make(chan uint64),
		waitQueryProofCh: make(chan *proofDataRecord),
		stopCh:           make(chan struct{}),
		waitStopCh:       make(chan error),
	}
	log.Info("Succeed to init proof keeper", "options", opts)
	return keeper
}

// Start is used to start event loop.
func (keeper *ProofKeeper) Start(blockChain *BlockChain, keeperMetaDB ethdb.Database) error {
	if !keeper.opts.enable {
		return nil
	}
	var (
		err        error
		ancientDir string
	)

	keeper.blockChain = blockChain
	keeper.keeperMetaDB = keeperMetaDB
	if ancientDir, err = keeper.keeperMetaDB.AncientDatadir(); err != nil {
		log.Error("Failed to get ancient data dir", "error", err)
		return err
	}
	if keeper.proofDataDB, err = rawdb.NewProofFreezer(ancientDir, false); err != nil {
		log.Error("Failed to new proof ancient freezer", "error", err)
		return err
	}

	go keeper.eventLoop()
	log.Info("Succeed to start proof keeper")
	return nil
}

// Stop is used to sync ancient db and stop the event loop.
func (keeper *ProofKeeper) Stop() error {
	if !keeper.opts.enable {
		return nil
	}

	close(keeper.stopCh)
	err := <-keeper.waitStopCh
	log.Info("Succeed to stop proof keeper", "error", err)
	return err
}

// GetKeepRecordWatchFunc returns a keeper callback func which is used by path db node buffer list.
// This is a synchronous operation.
func (keeper *ProofKeeper) GetKeepRecordWatchFunc() pathdb.KeepRecordWatchFunc {
	return func(keepRecord *pathdb.KeepRecord) {
		if keeper == nil {
			return
		}
		if keeper.opts == nil || keeper.opts.watchStartKeepCh == nil || keeper.opts.notifyFinishKeepCh == nil {
			return
		}
		if !keeper.opts.enable {
			return
		}
		if keepRecord.BlockID == 0 || keepRecord.KeepInterval == 0 {
			return
		}
		if keepRecord.BlockID%keepRecord.KeepInterval != 0 {
			return
		}

		startTimestamp := time.Now()
		defer func() {
			addProofTimer.UpdateSince(startTimestamp)
			log.Info("Succeed to keep proof", "record", keepRecord, "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
		}()

		keeper.opts.watchStartKeepCh <- keepRecord
		<-keeper.opts.notifyFinishKeepCh
	}
}

// getInnerProof is used to make proof by state db interface.
func (keeper *ProofKeeper) getInnerProof(kRecord *pathdb.KeepRecord) (*proofDataRecord, error) {
	var (
		err          error
		header       *types.Header
		stateDB      *state.StateDB
		worldTrie    *trie2.StateTrie
		accountProof common.ProofList
		pRecord      *proofDataRecord
	)

	startTimestamp := time.Now()
	defer func() {
		getInnerProofTimer.UpdateSince(startTimestamp)
		// log.Info("Succeed to get proof", "proof_record", pRecord, "error", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	}()

	if header = keeper.blockChain.GetHeaderByNumber(kRecord.BlockID); header == nil {
		return nil, fmt.Errorf("block is not found, block_id=%d", kRecord.BlockID)
	}
	if worldTrie, err = trie2.NewStateTrieByInnerReader(
		trie2.StateTrieID(header.Root),
		keeper.blockChain.stateCache.TrieDB(),
		kRecord.PinedInnerTrieReader); err != nil {
		return nil, err
	}
	if err = worldTrie.Prove(crypto.Keccak256(l2ToL1MessagePasserAddr.Bytes()), &accountProof); err != nil {
		return nil, err
	}
	if stateDB, err = state.NewStateDBByTrie(worldTrie, keeper.blockChain.stateCache, keeper.blockChain.snaps); err != nil {
		return nil, err
	}

	pRecord = &proofDataRecord{
		BlockID:      kRecord.BlockID,
		StateRoot:    kRecord.StateRoot,
		Address:      l2ToL1MessagePasserAddr,
		AccountProof: accountProof,
		Balance:      stateDB.GetBalance(l2ToL1MessagePasserAddr),
		CodeHash:     stateDB.GetCodeHash(l2ToL1MessagePasserAddr),
		Nonce:        stateDB.GetNonce(l2ToL1MessagePasserAddr),
		StorageHash:  stateDB.GetStorageRoot(l2ToL1MessagePasserAddr),
		StorageProof: make([]common.StorageResult, 0),
	}
	err = stateDB.Error()
	return pRecord, err
}

// eventLoop is used to update/query keeper meta and proof data in the event loop, which ensure thread-safe.
func (keeper *ProofKeeper) eventLoop() {
	if !keeper.opts.enable {
		return
	}
	var (
		err                     error
		putKeeperMetaRecordOnce bool
		ancientInitSequenceID   uint64
	)

	gcProofTicker := time.NewTicker(time.Second * time.Duration(keeper.opts.gcInterval))
	defer gcProofTicker.Stop()

	for {
		select {
		case keepRecord := <-keeper.opts.watchStartKeepCh:
			var (
				hasTruncatedMeta bool
				curProofID       uint64
				proofRecord      *proofDataRecord
			)

			proofRecord, err = keeper.getInnerProof(keepRecord)
			if err == nil {
				hasTruncatedMeta = keeper.truncateKeeperMetaRecordHeadIfNeeded(keepRecord.BlockID)
				metaList := keeper.getKeeperMetaRecordList()
				if len(metaList) == 0 {
					keeper.proofDataDB.Reset()
					curProofID = ancientInitSequenceID
				} else {
					keeper.truncateProofDataRecordHeadIfNeeded(keepRecord.BlockID)
					latestProofData := keeper.getLatestProofDataRecord()
					if latestProofData != nil {
						curProofID = latestProofData.ProofID + 1
					} else {
						curProofID = ancientInitSequenceID
					}
				}

				if hasTruncatedMeta || !putKeeperMetaRecordOnce {
					putKeeperMetaRecordOnce = true
					keeper.putKeeperMetaRecord(&keeperMetaRecord{
						BlockID:      keepRecord.BlockID,
						ProofID:      curProofID,
						KeepInterval: keepRecord.KeepInterval,
					})
				}
				proofRecord.ProofID = curProofID
				err = keeper.putProofDataRecord(proofRecord)
				keeper.latestBlockID = keepRecord.BlockID
			}
			log.Info("Succeed to keep a new proof",
				"block_id", keepRecord.BlockID,
				"state_root", keepRecord.StateRoot.String(),
				"error", err)
			keeper.opts.notifyFinishKeepCh <- struct{}{}

		case queryBlockID := <-keeper.queryProofCh:
			var resultProofRecord *proofDataRecord
			metaList := keeper.getKeeperMetaRecordList()
			if len(metaList) != 0 && (queryBlockID+keeper.opts.keepProofBlockSpan > keeper.latestBlockID) {
				proofID := uint64(0)
				index := len(metaList) - 1
				for index >= 0 {
					m := metaList[index]
					if queryBlockID >= m.BlockID {
						if m.KeepInterval == 0 || queryBlockID%m.KeepInterval != 0 { // check
							break
						}

						proofID = m.ProofID + (queryBlockID-m.BlockID)/m.KeepInterval
						resultProofRecord = keeper.getProofDataRecord(proofID)
						break
					}
					index = index - 1
				}
			}
			keeper.waitQueryProofCh <- resultProofRecord

		case <-keeper.stopCh:
			err = keeper.proofDataDB.Sync()
			if err == nil {
				err = keeper.proofDataDB.Close()
			}
			keeper.waitStopCh <- err
			return

		case <-gcProofTicker.C:
			log.Info("Start to gc proof", "latest_block_id", keeper.latestBlockID, "keep_block_span", keeper.opts.keepProofBlockSpan)
			if keeper.latestBlockID > keeper.opts.keepProofBlockSpan {
				gcBeforeBlockID := keeper.latestBlockID - keeper.opts.keepProofBlockSpan
				var gcBeforeKeepMetaRecord *keeperMetaRecord
				var gcBeforeProofDataRecord *proofDataRecord
				metaList := keeper.getKeeperMetaRecordList()
				proofID := uint64(0)
				if len(metaList) != 0 {
					index := len(metaList) - 1
					for index >= 0 {
						m := metaList[index]
						if gcBeforeBlockID >= m.BlockID {
							gcBeforeKeepMetaRecord = m
							proofID = m.ProofID + (gcBeforeBlockID-m.BlockID)/m.KeepInterval
							gcBeforeProofDataRecord = keeper.getProofDataRecord(proofID)
							break
						}
						index = index - 1
					}
				}
				keeper.gcKeeperMetaRecordIfNeeded(gcBeforeKeepMetaRecord)
				keeper.gcProofDataRecordIfNeeded(gcBeforeProofDataRecord)

			}
		}
	}
}

// getKeeperMetaRecordList returns keeper meta list.
func (keeper *ProofKeeper) getKeeperMetaRecordList() []*keeperMetaRecord {
	var (
		metaList []*keeperMetaRecord
		err      error
		iter     ethdb.Iterator
	)

	iter = rawdb.IterateKeeperMeta(keeper.keeperMetaDB)
	defer iter.Release()
	for iter.Next() {
		keyBlockID := binary.BigEndian.Uint64(iter.Key()[1:])
		m := keeperMetaRecord{}
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			log.Error("Failed to unmarshal keeper meta record", "key_block_id", keyBlockID, "error", err)
			continue
		}
		if keyBlockID != m.BlockID { // check
			log.Error("Failed to check consistency between key and value", "key_block_id", keyBlockID, "value_block_id", m.BlockID)
			continue
		}
		// log.Info("Keep meta", "key_block_id", keyBlockID, "meta_record", m)
		metaList = append(metaList, &m)
	}
	// log.Info("Succeed to get meta list", "list", metaList)
	return metaList
}

// truncateKeeperMetaRecordHeadIfNeeded is used to truncate keeper meta record head,
// which is used in reorg.
func (keeper *ProofKeeper) truncateKeeperMetaRecordHeadIfNeeded(blockID uint64) bool {
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
		if m.BlockID >= blockID {
			hasTruncated = true
			rawdb.DeleteKeeperMeta(batch, m.BlockID)
		}
	}
	err = batch.Write()
	if err != nil {
		log.Crit("Failed to truncate keeper meta head", "err", err)
	}
	// log.Info("Succeed to truncate keeper meta", "block_id", blockID, "has_truncated", hasTruncated)
	return hasTruncated
}

// putKeeperMetaRecord puts a new keeper meta record.
func (keeper *ProofKeeper) putKeeperMetaRecord(m *keeperMetaRecord) {
	meta, err := json.Marshal(*m)
	if err != nil {
		log.Crit("Failed to marshal keeper meta record", "err", err)
	}
	rawdb.PutKeeperMeta(keeper.keeperMetaDB, m.BlockID, meta)
	log.Info("Succeed to put keeper meta", "record", m)
}

// gcKeeperMetaRecordIfNeeded is used to the older keeper meta record.
func (keeper *ProofKeeper) gcKeeperMetaRecordIfNeeded(meta *keeperMetaRecord) {
	if !keeper.opts.enable {
		return
	}
	if meta == nil {
		return
	}
	metaList := keeper.getKeeperMetaRecordList()
	if len(metaList) < maxKeeperMetaNumber {
		return
	}

	var (
		err       error
		iter      ethdb.Iterator
		batch     ethdb.Batch
		gcCounter uint64
	)

	iter = rawdb.IterateKeeperMeta(keeper.keeperMetaDB)
	defer iter.Release()
	batch = keeper.keeperMetaDB.NewBatch()
	for iter.Next() {
		m := keeperMetaRecord{}
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			continue
		}
		if m.BlockID < meta.BlockID {
			rawdb.DeleteKeeperMeta(batch, m.BlockID)
			gcCounter++
		}
	}
	err = batch.Write()
	if err != nil {
		log.Crit("Failed to gc keeper meta", "err", err)
	}
	log.Info("Succeed to gc keeper meta", "gc_before_keep_meta", meta, "gc_counter", gcCounter)
}

// truncateProofDataRecordHeadIfNeeded is used to truncate proof data record head,
// which is used in reorg.
func (keeper *ProofKeeper) truncateProofDataRecordHeadIfNeeded(blockID uint64) {
	latestProofDataRecord := keeper.getLatestProofDataRecord()
	if latestProofDataRecord == nil {
		log.Info("Skip to truncate proof data due to proof data is empty")
		return
	}
	if blockID > latestProofDataRecord.BlockID {
		// log.Info("Skip to truncate proof data due to block id is newer")
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
	log.Info("Succeed to truncate proof data", "block_id", blockID, "truncate_proof_id", truncateProofID)
}

// getLatestProofDataRecord return the latest proof data record.
func (keeper *ProofKeeper) getLatestProofDataRecord() *proofDataRecord {
	latestProofData := rawdb.GetLatestProofData(keeper.proofDataDB)
	if latestProofData == nil {
		log.Info("Skip get latest proof data record due to empty")
		return nil
	}
	var data proofDataRecord
	err := json.Unmarshal(latestProofData, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	// log.Info("Succeed to get latest proof data", "record", data)
	return &data
}

// getProofDataRecord returns proof record by proofid.
func (keeper *ProofKeeper) getProofDataRecord(proofID uint64) *proofDataRecord {
	latestProofData := rawdb.GetProofData(keeper.proofDataDB, proofID)
	if latestProofData == nil {
		log.Info("Skip get proof data record due not found", "proof_id", proofID)
		return nil
	}
	var data proofDataRecord
	err := json.Unmarshal(latestProofData, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	// log.Info("Succeed to get proof data", "record", data)
	return &data
}

// putProofDataRecord puts a new proof data record.
func (keeper *ProofKeeper) putProofDataRecord(p *proofDataRecord) error {
	proof, err := json.Marshal(*p)
	if err != nil {
		log.Error("Failed to marshal proof data", "error", err)
		return err
	}
	err = rawdb.PutProofData(keeper.proofDataDB, p.ProofID, proof)
	// log.Info("Succeed to put proof data", "record", p, "error", err)
	return err
}

// gcProofDataRecordIfNeeded is used to the older proof data record.
func (keeper *ProofKeeper) gcProofDataRecordIfNeeded(data *proofDataRecord) {
	if !keeper.opts.enable {
		return
	}
	if data == nil {
		return
	}
	if data.ProofID == 0 {
		return
	}

	rawdb.TruncateProofDataTail(keeper.proofDataDB, data.ProofID)
	log.Info("Succeed to gc proof data", "gc_before_proof_data", data)
}

// IsProposeProofQuery is used to determine whether it is proposed proof.
func (keeper *ProofKeeper) IsProposeProofQuery(address common.Address, storageKeys []string, blockID uint64) bool {
	if !keeper.opts.enable {
		return false
	}
	if l2ToL1MessagePasserAddr.Cmp(address) != 0 {
		return false
	}
	if len(storageKeys) != 0 {
		return false
	}
	// blockID%keepInterval == 0 is not checked because keepInterval may have been adjusted before.
	_ = blockID
	return true
}

// QueryProposeProof is used to get proof which is stored in ancient proof.
func (keeper *ProofKeeper) QueryProposeProof(blockID uint64, stateRoot common.Hash) (*common.AccountResult, error) {
	var (
		result         *common.AccountResult
		err            error
		startTimestamp time.Time
	)

	startTimestamp = time.Now()
	defer func() {
		queryProofTimer.UpdateSince(startTimestamp)
		log.Info("Query propose proof",
			"block_id", blockID,
			"state_root", stateRoot.String(),
			"error", err, "elapsed", common.PrettyDuration(time.Since(startTimestamp)))
	}()

	keeper.queryProofCh <- blockID
	resultProofRecord := <-keeper.waitQueryProofCh
	if resultProofRecord == nil {
		// Maybe the keeper was disabled for a certain period of time before.
		err = fmt.Errorf("proof is not found, block_id=%d", blockID)
		return nil, err
	}
	if resultProofRecord.BlockID != blockID {
		// Maybe expected_block_id proof is not kept due to disabled or some bug
		err = fmt.Errorf("proof is not found due to block is mismatch, expected_block_id=%d, actual_block_id=%d",
			blockID, resultProofRecord.BlockID)
		return nil, err
	}
	if resultProofRecord.StateRoot.Cmp(stateRoot) != 0 {
		// Impossible, unless there is a bug.
		err = fmt.Errorf("proof is not found due to state root is mismatch, expected_state_root=%s, actual_state_root=%s",
			stateRoot.String(), resultProofRecord.StateRoot.String())
		return nil, err
	}
	result = &common.AccountResult{
		Address:      resultProofRecord.Address,
		AccountProof: resultProofRecord.AccountProof,
		Balance:      resultProofRecord.Balance,
		CodeHash:     resultProofRecord.CodeHash,
		Nonce:        resultProofRecord.Nonce,
		StorageHash:  resultProofRecord.StorageHash,
		StorageProof: resultProofRecord.StorageProof,
	}
	return result, nil
}
