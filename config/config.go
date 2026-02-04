package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port          int            `json:"port" yaml:"port"`
	Database      DatabaseConfig `json:"database" yaml:"database"`
	S3Credentials []S3Config     `json:"s3_credentials" yaml:"s3_credentials"`
}

type DatabaseConfig struct {
	Sqlite   *SqliteConfig   `json:"sqlite,omitempty" yaml:"sqlite,omitempty"`
	Postgres *PostgresConfig `json:"postgres,omitempty" yaml:"postgres,omitempty"`
}

type SqliteConfig struct {
	File string `json:"file" yaml:"file"`
}

type PostgresConfig struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database" yaml:"database"`
	SSLMode  string `json:"sslmode" yaml:"sslmode"`
}

type S3Config struct {
	Bucket    string `json:"bucket" yaml:"bucket"`
	Region    string `json:"region" yaml:"region"`
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key" yaml:"secret_key"`
	Endpoint  string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
}

func LoadConfig(configFile string) (*Config, error) {
	// 1. Default Config
	cfg := &Config{
		Port:     8080,
		Database: DatabaseConfig{},
	}

	// 2. Load from file if provided
	if configFile != "" {
		f, err := os.Open(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to open config file: %w", err)
		}
		defer f.Close()

		ext := filepath.Ext(configFile)
		if ext == ".yaml" || ext == ".yml" {
			if err := yaml.NewDecoder(f).Decode(cfg); err != nil {
				return nil, fmt.Errorf("failed to decode yaml config: %w", err)
			}
		} else if ext == ".json" {
			if err := json.NewDecoder(f).Decode(cfg); err != nil {
				return nil, fmt.Errorf("failed to decode json config: %w", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported config file extension: %s", ext)
		}
	}

	// 3. Override with Environment Variables (if set)
	if portStr := os.Getenv("DRS_PORT"); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %s", portStr)
		}
		cfg.Port = p
	}

	// DB Env Vars overrides
	// If Postgres env vars are provided, we assume Postgres.
	if os.Getenv("DRS_DB_HOST") != "" || os.Getenv("DRS_DB_DATABASE") != "" {
		if cfg.Database.Postgres == nil {
			cfg.Database.Postgres = &PostgresConfig{
				Host:    "localhost",
				Port:    5432,
				SSLMode: "disable",
			}
		}
		// If env vars specify postgres, we should probably disable the default sqlite if it was still active
		// But let's let the validation catch it if they are both set.
	}

	if cfg.Database.Postgres != nil {
		if v := os.Getenv("DRS_DB_HOST"); v != "" {
			cfg.Database.Postgres.Host = v
		}
		if v := os.Getenv("DRS_DB_PORT"); v != "" {
			p, err := strconv.Atoi(v)
			if err == nil {
				cfg.Database.Postgres.Port = p
			}
		}
		if v := os.Getenv("DRS_DB_USER"); v != "" {
			cfg.Database.Postgres.User = v
		}
		if v := os.Getenv("DRS_DB_PASSWORD"); v != "" {
			cfg.Database.Postgres.Password = v
		}
		if v := os.Getenv("DRS_DB_DATABASE"); v != "" {
			cfg.Database.Postgres.Database = v
		}
		if v := os.Getenv("DRS_DB_SSLMODE"); v != "" {
			cfg.Database.Postgres.SSLMode = v
		}
	}

	if v := os.Getenv("DRS_DB_SQLITE_FILE"); v != "" {
		if cfg.Database.Sqlite == nil {
			cfg.Database.Sqlite = &SqliteConfig{}
		}
		cfg.Database.Sqlite.File = v
	}

	// Final Validation: Exactly one DB must be specified
	if cfg.Database.Sqlite != nil && cfg.Database.Postgres != nil {
		// If both are set, but one is the default "drs.db" and the other was explicitly set by user,
		// we can try to be smart, but user asked to "raise an error".
		// Actually, if I load a file that has `postgres:`, the `sqlite:` default from line 52 is still there.
		// So I must clear it if postgres is detected.

		// If postgres was explicitly defined (either in file or via env), we clear the default sqlite.
		// A better way is to check if it's the "default" value.
		if cfg.Database.Sqlite.File == "drs.db" && (cfg.Database.Postgres.Host != "localhost" || cfg.Database.Postgres.Database != "") {
			// This is risky. Let's just follow the user instruction: if both present, error.
			// This means my LoadConfig must be careful not to leave defaults if others are set.
		}
	}

	if cfg.Database.Sqlite != nil && cfg.Database.Postgres != nil {
		return nil, fmt.Errorf("multiple databases specified in config; only one of 'sqlite' or 'postgres' allowed")
	}
	if cfg.Database.Sqlite == nil && cfg.Database.Postgres == nil {
		return nil, fmt.Errorf("no database specified in config")
	}

	// Validate S3 Credentials
	for i, cred := range cfg.S3Credentials {
		if cred.Bucket == "" {
			return nil, fmt.Errorf("s3_credentials[%d]: bucket is required", i)
		}
		if cred.Region == "" {
			return nil, fmt.Errorf("s3_credentials[%d]: region is required", i)
		}
		if cred.AccessKey == "" {
			return nil, fmt.Errorf("s3_credentials[%d]: access_key is required", i)
		}
		if cred.SecretKey == "" {
			return nil, fmt.Errorf("s3_credentials[%d]: secret_key is required", i)
		}
	}

	return cfg, nil
}
