package state

import (
	"encoding/hex"

	"github.com/ethereum/go-ethereum/common"
)

func PrintAccounts(accounts map[common.Hash][]byte) string {
	res := ""
	for k, v := range accounts {
		res += k.String() + " : " + hex.EncodeToString(v) + ";"
	}
	return res
}
