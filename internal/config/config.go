package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration parameters for ChronoSaga.
type Config struct {
	DatabaseURL         string
	DBMaxOpenConns      int
	DBMaxIdleConns      int
	DBConnMaxLifetime   time.Duration
	Host                string
	Port                string
	Environment         string
	LogLevel            string
	AdminAPIKey         string
	WorkerPollInterval  time.Duration
	WorkerConcurrency   int
	WorkerLeaseDuration time.Duration
	TimerPollInterval   time.Duration
}

// LoadConfig reads configuration from environment variables with sensible defaults.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:         getEnv("DATABASE_URL", "postgres://chronosaga:chronosaga_secret@localhost:5432/chronosaga?sslmode=disable"),
		DBMaxOpenConns:      getEnvInt("DB_MAX_OPEN_CONNS", 25),
		DBMaxIdleConns:      getEnvInt("DB_MAX_IDLE_CONNS", 10),
		DBConnMaxLifetime:   getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		Host:                getEnv("CHRONOSAGA_HOST", "127.0.0.1"),
		Port:                getEnv("CHRONOSAGA_PORT", "8080"),
		Environment:         getEnv("CHRONOSAGA_ENVIRONMENT", "development"),
		LogLevel:            getEnv("CHRONOSAGA_LOG_LEVEL", "info"),
		AdminAPIKey:         os.Getenv("CHRONOSAGA_ADMIN_API_KEY"),
		WorkerPollInterval:  getEnvDuration("WORKER_POLL_INTERVAL", 300*time.Millisecond),
		WorkerConcurrency:   getEnvInt("WORKER_CONCURRENCY", 5),
		WorkerLeaseDuration: getEnvDuration("WORKER_LEASE_DURATION", 30*time.Second),
		TimerPollInterval:   getEnvDuration("TIMER_POLL_INTERVAL", 200*time.Millisecond),
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks whether the configuration meets system constraints.
func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	if c.Port == "" {
		return errors.New("CHRONOSAGA_PORT is required")
	}
	if c.WorkerConcurrency <= 0 {
		return errors.New("WORKER_CONCURRENCY must be greater than zero")
	}
	if c.WorkerLeaseDuration <= 0 {
		return errors.New("WORKER_LEASE_DURATION must be positive")
	}
	if c.WorkerPollInterval <= 0 {
		return errors.New("WORKER_POLL_INTERVAL must be positive")
	}
	if c.TimerPollInterval <= 0 {
		return errors.New("TIMER_POLL_INTERVAL must be positive")
	}
	return nil
}

// RedactedDatabaseURL returns the connection URL with sensitive credentials masked.
func (c *Config) RedactedDatabaseURL() string {
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		return "[REDACTED_INVALID_URL]"
	}
	return u.Redacted()
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if parsed, err := time.ParseDuration(val); err == nil {
			return parsed
		}
	}
	return defaultVal
}
