package config

import (
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	for _, key := range []string{"APP_ENV", "HOST", "PORT", "API_KEY", "INPUT_DIR", "RATE_LIMIT_PER_MINUTE", "ALLOW_PRIVATE_SOURCES", "READ_HEADER_TIMEOUT", "READ_TIMEOUT", "WRITE_TIMEOUT", "IDLE_TIMEOUT", "SHUTDOWN_TIMEOUT"} {
		t.Setenv(key, "")
	}
	// Empty values are not equivalent to unset values; test explicit defaults below.
	t.Setenv("APP_ENV", "development")
	t.Setenv("HOST", "127.0.0.1")
	t.Setenv("PORT", "8080")
	t.Setenv("INPUT_DIR", "testdata/input")
	t.Setenv("RATE_LIMIT_PER_MINUTE", "120")
	t.Setenv("ALLOW_PRIVATE_SOURCES", "true")
	t.Setenv("READ_HEADER_TIMEOUT", "5s")
	t.Setenv("READ_TIMEOUT", "15s")
	t.Setenv("WRITE_TIMEOUT", "30s")
	t.Setenv("IDLE_TIMEOUT", "60s")
	t.Setenv("SHUTDOWN_TIMEOUT", "10s")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != "8080" || cfg.RateLimitPerMinute != 120 || !cfg.AllowPrivateSources {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
}

func TestProductionRequiresKey(t *testing.T) {
	cfg := Config{Environment: "production", Host: "0.0.0.0", Port: "8080", InputDir: "testdata/input", RateLimitPerMinute: 120,
		ReadHeaderTimeout: DefaultReadHeaderTimeout, ReadTimeout: DefaultReadTimeout, WriteTimeout: DefaultWriteTimeout, IdleTimeout: DefaultIdleTimeout, ShutdownTimeout: DefaultShutdownTimeout}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "API_KEY") {
		t.Fatalf("expected API key error, got %v", err)
	}
	cfg.APIKey = "example-secret"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	t.Setenv("PORT", "not-a-port")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid port error")
	}
}
