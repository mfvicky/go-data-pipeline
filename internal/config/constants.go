package config

import "time"

const (
	DefaultHost               = "127.0.0.1"
	DefaultPort               = "8080"
	DefaultInputDir           = "testdata/input"
	DefaultRateLimitPerMinute = 120
	DefaultReadHeaderTimeout  = 5 * time.Second
	DefaultReadTimeout        = 15 * time.Second
	DefaultWriteTimeout       = 30 * time.Second
	DefaultIdleTimeout        = 60 * time.Second
	DefaultShutdownTimeout    = 10 * time.Second
)
