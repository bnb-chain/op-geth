package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const (
	blockNumberLength = 8 // uint64 is 8 bytes.
)

// IterateKeeperMeta returns keep meta iterator.
func IterateKeeperMeta(db ethdb.Iteratee) ethdb.Iterator {
	return NewKeyLengthIterator(db.NewIterator(proofKeeperMetaPrefix, nil), len(proofKeeperMetaPrefix)+blockNumberLength)
}

// DeleteKeeperMeta is used to remove the specified keeper meta.
func DeleteKeeperMeta(db ethdb.KeyValueWriter, blockID uint64) {
	if err := db.Delete(proofKeeperMetaKey(blockID)); err != nil {
		log.Crit("Failed to delete keeper meta", "err", err)
	}
}

// PutKeeperMeta add a new keeper meta.
func PutKeeperMeta(db ethdb.KeyValueWriter, blockID uint64, meta []byte) {
	key := proofKeeperMetaKey(blockID)
	if err := db.Put(key, meta); err != nil {
		log.Crit("Failed to store keeper meta", "err", err)
	}
}

// GetLatestProofData returns the latest head proof data.
func GetLatestProofData(f *ResettableFreezer) []byte {
	proofTable := f.freezer.tables[proposeProofTable]
	if proofTable == nil {
		return nil
	}
	blob, err := f.Ancient(proposeProofTable, proofTable.items.Load()-1)
	if err != nil {
		log.Error("Failed to get latest proof data", "latest_proof_id", proofTable.items.Load()-1, "error", err)
		return nil
	}
	return blob
}

// GetProofData returns the specified proof data.
func GetProofData(f *ResettableFreezer, proofID uint64) []byte {
	proofTable := f.freezer.tables[proposeProofTable]
	if proofTable == nil {
		return nil
	}
	blob, err := f.Ancient(proposeProofTable, proofID)
	if err != nil {
		return nil
	}
	return blob
}

// TruncateProofDataHead truncates [proofID, end...].
func TruncateProofDataHead(f *ResettableFreezer, proofID uint64) {
	f.freezer.TruncateHead(proofID)
}

// TruncateProofDataTail truncates [start..., proofID).
func TruncateProofDataTail(f *ResettableFreezer, proofID uint64) {
	f.freezer.TruncateTail(proofID)
}

// PutProofData appends a new proof to ancient proof db, the proofID should be continuous.
func PutProofData(db ethdb.AncientWriter, proofID uint64, proof []byte) error {
	_, err := db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		return op.AppendRaw(proposeProofTable, proofID, proof)
	})
	return err
}
