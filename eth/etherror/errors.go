package etherror

import "errors"

var (
	ErrNoHeadersDelivered         = errors.New("no headers delivered")
	ErrInvalidHeaderBatchAnchor   = errors.New("invalid header batch anchor")
	ErrNotEnoughNonGenesisHeaders = errors.New("not enough non-genesis headers delivered")
	ErrNotEnoughGenesisHeaders    = errors.New("not enough genesis headers delivered")
	ErrInvalidHashProgression     = errors.New("invalid hash progression")
)
