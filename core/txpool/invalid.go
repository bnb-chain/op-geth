package txpool

import (
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	TypeNotSupportDeposit   = "TypeNotSupportDeposit"
	TypeNotSupport1559      = "TypeNotSupport1559"
	TypeNotSupport2718      = "TypeNotSupport2718"
	MissingTransaction      = "MissingTransaction"
	OversizedData           = "OversizedData"
	MaxInitCodeSizeExceeded = "MaxInitCodeSizeExceeded"
	NegativeValue           = "NegativeValue"
	GasLimit                = "GasLimit"
	FeeCapVeryHigh          = "FeeCapVeryHigh"
	TipVeryHigh             = "TipVeryHigh"
	TipAboveFeeCap          = "TipAboveFeeCap"
	InvalidSender           = "InvalidSender"
	Underpriced             = "Underpriced"
	NonceTooLow             = "NonceTooLow"
	InsufficientFunds       = "InsufficientFunds"
	Overdraft               = "Overdraft"
	IntrinsicGas            = "IntrinsicGas"
	FutureReplacePending    = "FutureReplacePending"
	GasUnitOverflow         = "GasUnitOverflow"
)

func Meter(err string) metrics.Meter {
	return meter(err)
}

func meter(err string) metrics.Meter {
	return metrics.GetOrRegisterMeter("txpool/invalid/"+err, nil)
}

func init() {
	// init the metrics
	for _, err := range []string{
		TypeNotSupportDeposit,
		TypeNotSupport1559,
		TypeNotSupport2718,
		MissingTransaction,
		OversizedData,
		MaxInitCodeSizeExceeded,
		NegativeValue,
		GasLimit,
		FeeCapVeryHigh,
		TipVeryHigh,
		TipAboveFeeCap,
		InvalidSender,
		Underpriced,
		NonceTooLow,
		InsufficientFunds,
		Overdraft,
		IntrinsicGas,
		FutureReplacePending,
		GasUnitOverflow,
	} {
		meter(err).Mark(0)
	}
}
