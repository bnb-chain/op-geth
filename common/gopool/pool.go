package gopool

import (
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/panjf2000/ants/v2"
)

const (
	// DefaultAntsPoolSize is the default expire time of ants pool.
	defaultGoroutineExpireDuration = 10 * time.Second
)

var (
	// defaultPool is the default ants pool for gopool package.
	defaultPool *ants.Pool
)

func init() {
	// Init a instance pool when importing ants.
	pool, err := ants.NewPool(
		ants.DefaultAntsPoolSize,
		ants.WithExpiryDuration(defaultGoroutineExpireDuration),
	)
	if err != nil {
		panic(err)
	}

	defaultPool = pool
}

// Submit submits a task to pool.
func Submit(task func()) {
	err := defaultPool.Submit(task)
	if err != nil {
		log.Error("pool submit task fail", "err", err, "task", task)
	}
}

// Running returns the number of the currently running goroutines.
func Running() int {
	return defaultPool.Running()
}

// Cap returns the capacity of this default pool.
func Cap() int {
	return defaultPool.Cap()
}

// Free returns the available goroutines to work.
func Free() int {
	return defaultPool.Free()
}

// Release Closes the default pool.
func Release() {
	defaultPool.Release()
}

// Reboot reboots the default pool.
func Reboot() {
	defaultPool.Reboot()
}
