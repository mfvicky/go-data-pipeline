package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Environment         string
	Host                string
	Port                string
	APIKey              string
	InputDir            string
	AllowPrivateSources bool
	RateLimitPerMinute  int
	MaxConcurrentJobs   int
	JobQueueCapacity    int
	JobRetention        time.Duration
	StorageDriver       string
	QueueDriver         string
	RabbitMQURL         string
	RabbitMQQueue       string
	SourceHostAllowlist []string
	DatabaseURL         string
	LogLevel            string
	LogFormat           string
	CORSOrigins         []string
	ReadHeaderTimeout   time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	ShutdownTimeout     time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Environment:         strings.ToLower(getenv("APP_ENV", "development")),
		Host:                getenv("HOST", DefaultHost),
		Port:                getenv("PORT", DefaultPort),
		APIKey:              os.Getenv("API_KEY"),
		InputDir:            getenv("INPUT_DIR", DefaultInputDir),
		StorageDriver:       strings.ToLower(getenv("STORAGE_DRIVER", "memory")),
		QueueDriver:         strings.ToLower(getenv("QUEUE_DRIVER", "memory")),
		RabbitMQURL:         os.Getenv("RABBITMQ_URL"),
		RabbitMQQueue:       getenv("RABBITMQ_QUEUE", "pipeline.jobs"),
		SourceHostAllowlist: parseOrigins(os.Getenv("SOURCE_HOST_ALLOWLIST")),
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		LogLevel:            strings.ToLower(getenv("LOG_LEVEL", "info")),
		LogFormat:           strings.ToLower(getenv("LOG_FORMAT", "json")),
		CORSOrigins:         parseOrigins(os.Getenv("CORS_ORIGINS")),
	}
	var err error
	if cfg.RateLimitPerMinute, err = readInt("RATE_LIMIT_PER_MINUTE", DefaultRateLimitPerMinute); err != nil {
		return cfg, err
	}
	if cfg.MaxConcurrentJobs, err = readInt("MAX_CONCURRENT_JOBS", 5); err != nil {
		return cfg, err
	}
	if cfg.JobQueueCapacity, err = readInt("JOB_QUEUE_CAPACITY", 100); err != nil {
		return cfg, err
	}
	if cfg.JobRetention, err = readDuration("JOB_RETENTION", 24*time.Hour); err != nil {
		return cfg, err
	}
	if cfg.AllowPrivateSources, err = readBool("ALLOW_PRIVATE_SOURCES", cfg.Environment != "production"); err != nil {
		return cfg, err
	}
	for _, entry := range []struct {
		name         string
		target       *time.Duration
		defaultValue time.Duration
	}{
		{"READ_HEADER_TIMEOUT", &cfg.ReadHeaderTimeout, DefaultReadHeaderTimeout},
		{"READ_TIMEOUT", &cfg.ReadTimeout, DefaultReadTimeout},
		{"WRITE_TIMEOUT", &cfg.WriteTimeout, DefaultWriteTimeout},
		{"IDLE_TIMEOUT", &cfg.IdleTimeout, DefaultIdleTimeout},
		{"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout, DefaultShutdownTimeout},
	} {
		if *entry.target, err = readDuration(entry.name, entry.defaultValue); err != nil {
			return cfg, err
		}
	}
	return cfg, cfg.Validate()
}

func (cfg Config) Validate() error {
	if cfg.Environment != "development" && cfg.Environment != "production" {
		return fmt.Errorf("APP_ENV must be development or production")
	}
	port, err := strconv.Atoi(cfg.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("PORT must be between 1 and 65535")
	}
	if strings.TrimSpace(cfg.Host) == "" || strings.ContainsAny(cfg.Host, ":/ ") && net.ParseIP(cfg.Host) == nil {
		return fmt.Errorf("HOST must be a hostname or IP address")
	}
	if cfg.MaxConcurrentJobs < 0 || cfg.MaxConcurrentJobs > 64 {
		return fmt.Errorf("MAX_CONCURRENT_JOBS must be between 1 and 64")
	}
	if cfg.JobQueueCapacity < 0 || cfg.JobQueueCapacity > 10000 {
		return fmt.Errorf("JOB_QUEUE_CAPACITY must be between 1 and 10000")
	}
	if cfg.JobRetention < 0 {
		return fmt.Errorf("JOB_RETENTION must be nonnegative")
	}
	if cfg.QueueDriver != "" && cfg.QueueDriver != "memory" && cfg.QueueDriver != "rabbitmq" {
		return fmt.Errorf("QUEUE_DRIVER must be memory or rabbitmq")
	}
	if cfg.QueueDriver == "rabbitmq" && (cfg.StorageDriver != "postgres" || cfg.RabbitMQURL == "") {
		return fmt.Errorf("RabbitMQ requires STORAGE_DRIVER=postgres and RABBITMQ_URL")
	}
	if cfg.StorageDriver != "" && cfg.StorageDriver != "memory" && cfg.StorageDriver != "postgres" {
		return fmt.Errorf("STORAGE_DRIVER must be memory or postgres")
	}
	if cfg.StorageDriver == "postgres" && strings.TrimSpace(cfg.DatabaseURL) == "" && strings.TrimSpace(os.Getenv("PGHOST")) == "" {
		return fmt.Errorf("DATABASE_URL or PGHOST is required when STORAGE_DRIVER=postgres")
	}
	if cfg.LogLevel != "" && cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return fmt.Errorf("invalid LOG_LEVEL")
	}
	if cfg.LogFormat != "" && cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		return fmt.Errorf("invalid LOG_FORMAT")
	}
	if cfg.RateLimitPerMinute < 1 || cfg.RateLimitPerMinute > 100000 {
		return fmt.Errorf("RATE_LIMIT_PER_MINUTE must be between 1 and 100000")
	}
	if cfg.ReadHeaderTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.IdleTimeout <= 0 || cfg.ShutdownTimeout <= 0 {
		return fmt.Errorf("server timeouts must be positive")
	}
	if strings.TrimSpace(cfg.InputDir) == "" {
		return fmt.Errorf("INPUT_DIR must not be empty")
	}
	if cfg.Environment == "production" && cfg.APIKey == "" {
		return fmt.Errorf("API_KEY is required in production")
	}
	if !isLoopbackHost(cfg.Host) && cfg.APIKey == "" {
		return fmt.Errorf("API_KEY is required when HOST is not loopback")
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func getenv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func readInt(key string, fallback int) (int, error) {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func readBool(key string, fallback bool) (bool, error) {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback, nil
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

func readDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func parseOrigins(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	result := make([]string, 0)
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			result = append(result, entry)
		}
	}
	return result
}
