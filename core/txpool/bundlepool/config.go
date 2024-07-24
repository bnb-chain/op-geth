package bundlepool

import (
	"github.com/ethereum/go-ethereum/log"
)

type Config struct {
	GlobalSlots uint64 // Maximum number of bundle slots for all accounts
}

// DefaultConfig contains the default configurations for the bundle pool.
var DefaultConfig = Config{
	GlobalSlots: 4096,
}

// sanitize checks the provided user configurations and changes anything that's
// unreasonable or unworkable.
func (config *Config) sanitize() Config {
	conf := *config
	if conf.GlobalSlots < 1 {
		log.Warn("Sanitizing invalid bundlepool bundle slots", "provided", conf.GlobalSlots, "updated", DefaultConfig.GlobalSlots)
		conf.GlobalSlots = DefaultConfig.GlobalSlots
	}
	return conf
}
