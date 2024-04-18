package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const (
	blockNumberLength = 8 // uint64 is 8bytes
)

// Keeper Meta
func IterateKeeperMeta(db ethdb.Iteratee) ethdb.Iterator {
	return NewKeyLengthIterator(db.NewIterator(proofKeeperMetaPrefix, nil), len(proofKeeperMetaPrefix)+blockNumberLength)
}

func DeleteKeeperMeta(db ethdb.KeyValueWriter, blockID uint64) {
	if err := db.Delete(proofKeeperMetaKey(blockID)); err != nil {
		log.Crit("Failed to delete keeper meta", "err", err)
	}
}

func PutKeeperMeta(db ethdb.KeyValueWriter, blockID uint64, meta []byte) {
	key := proofKeeperMetaKey(blockID)
	if err := db.Put(key, meta); err != nil {
		log.Crit("Failed to store keeper meta", "err", err)
	}
}

// Proof Data
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

func TruncateProofDataHead(f *ResettableFreezer, proofID uint64) {
	f.freezer.TruncateHead(proofID)
}

func PutProofData(db ethdb.AncientWriter, proofID uint64, proof []byte) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		err := op.AppendRaw(proposeProofTable, proofID, proof)
		if err != nil {
			// todo: panic
			log.Error("Failed to put proof data", "proof_id", proofID, "error", err)
		}
		return nil
	})
}
