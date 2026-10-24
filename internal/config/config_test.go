package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear any overrides
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("CHRONOSAGA_PORT")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading default config: %v", err)
	}

	if cfg.Port != "8080" {
		t.Errorf("expected port 8080, got %s", cfg.Port)
	}
	if cfg.WorkerConcurrency != 5 {
		t.Errorf("expected worker concurrency 5, got %d", cfg.WorkerConcurrency)
	}
	if cfg.WorkerLeaseDuration != 30*time.Second {
		t.Errorf("expected lease duration 30s, got %v", cfg.WorkerLeaseDuration)
	}
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{
			name: "missing database url",
			mutate: func(c *Config) {
				c.DatabaseURL = ""
			},
			wantErr: true,
		},
		{
			name: "missing port",
			mutate: func(c *Config) {
				c.Port = ""
			},
			wantErr: true,
		},
		{
			name: "zero concurrency",
			mutate: func(c *Config) {
				c.WorkerConcurrency = 0
			},
			wantErr: true,
		},
		{
			name: "zero lease duration",
			mutate: func(c *Config) {
				c.WorkerLeaseDuration = 0
			},
			wantErr: true,
		},
		{
			name: "zero worker poll interval",
			mutate: func(c *Config) {
				c.WorkerPollInterval = 0
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			tt.mutate(cfg)
			err = cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
