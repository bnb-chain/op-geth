package objs

import (
	"math/big"
	"sync"
)

var (
	BigIntPool = sync.Pool{
		New: func() any {
			return new(big.Int)
		},
	}
)
