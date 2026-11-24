// Copyright 2025 The op-geth Authors
// This file is part of the op-geth library.
//
// The op-geth library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The op-geth library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the op-geth library. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// TotalTxCost returns the transaction's total cost, including execution and L1
// data availability costs. Only execution costs apply if l1CostFn is nil.
// This backport retains the L1 cost interface; this chain has no operator fee.
func TotalTxCost(tx *types.Transaction, l1CostFn L1CostFunc) (*uint256.Int, bool) {
	cost, overflow := uint256.FromBig(tx.Cost())
	if overflow {
		return nil, true
	} else if l1CostFn == nil {
		return cost, false
	}
	l1Cost := l1CostFn(tx.RollupCostData())
	if l1Cost == nil {
		return cost, false
	}
	rollupCost, overflow := uint256.FromBig(l1Cost)
	if overflow {
		return nil, true
	}
	return cost.AddOverflow(cost, rollupCost)
}
