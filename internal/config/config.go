package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/calypr/syfon/internal/common"
	"gopkg.in/yaml.v3"
)

const (
	S3Prefix        = common.S3Prefix
	GCSPrefix       = common.GCSPrefix
	AzurePrefix     = common.AzurePrefix
	DRSPrefix       = common.DRSPrefix
	DefaultS3Region = "us-east-1"

	DefaultSigningExpirySeconds = 900 // 15 minutes

	// --- Route Constants ---
	RouteHealthz = "/healthz"

	// LFS
	RouteLFSBatch    = "/info/lfs/objects/batch"
	RouteLFSMetadata = "/info/lfs/objects/metadata"
	RouteLFSObject   = "/info/lfs/objects/{oid}"
	RouteLFSVerify   = "/info/lfs/verify"

	// Metrics
	RouteMetricsFiles      = "/index/v1/metrics/files"
	RouteMetricsFileDetail = "/index/v1/metrics/files/{object_id}"
	RouteMetricsSummary    = "/index/v1/metrics/summary"

	// Docs
	RouteSwaggerUI    = "/index/swagger"
	RouteSwaggerUIAlt = "/index/swagger/"
	RouteOpenAPISpec  = "/index/openapi.yaml"
	RouteLFSSpec      = "/index/openapi-lfs.yaml"
	RouteBucketSpec   = "/index/openapi-bucket.yaml"
	RouteInternalSpec = "/index/openapi-internal.yaml"

	// Internal DRS Data
	RouteInternalDownload          = "/data/download/{file_id}"
	RouteInternalDownloadPart      = "/data/download/{file_id}/part"
	RouteInternalUpload            = "/data/upload"
	RouteInternalUploadURL         = "/data/upload/{file_id}"
	RouteInternalUploadBulk        = "/data/upload/bulk"
	RouteInternalMultipartInit     = "/data/multipart/init"
	RouteInternalMultipartUpload   = "/data/multipart/upload"
	RouteInternalMultipartComplete = "/data/multipart/complete"
	RouteInternalBuckets           = "/data/buckets"
	RouteInternalBucketDetail      = "/data/buckets/{bucket}"
	RouteInternalBucketScopes      = "/data/buckets/{bucket}/scopes"

	// Internal DRS Index
	RouteInternalIndex            = "/index"
	RouteInternalIndexDetail      = "/index/{id}"
	RouteInternalBulkHashes       = "/index/bulk/hashes"
	RouteInternalBulkDeleteHashes = "/index/bulk/delete"
	RouteInternalBulkSHA256       = "/index/bulk/sha256/validity"
	RouteInternalBulkCreate       = "/index/bulk"
	RouteInternalBulkDocs         = "/index/bulk/documents"

	// Core API
	RouteCoreSHA256 = "/index/v1/sha256/validity"
)

const (
	AuthModeLocal = "local"
	AuthModeGen3  = "gen3"
)

type Config struct {
	Port          int            `json:"port" yaml:"port"`
	Database      DatabaseConfig `json:"database" yaml:"database"`
	S3Credentials []S3Config     `json:"s3_credentials" yaml:"s3_credentials"`
	Auth          AuthConfig     `json:"auth" yaml:"auth"`
	LFS           LFSConfig      `json:"lfs" yaml:"lfs"`
	Signing       SigningConfig  `json:"signing" yaml:"signing"`
	Routes        RoutesConfig   `json:"routes" yaml:"routes"`
}

type RoutesConfig struct {
	Docs     bool `json:"docs" yaml:"docs"`
	Ga4gh    bool `json:"ga4gh" yaml:"ga4gh"`
	Metrics  bool `json:"metrics" yaml:"metrics"`
	Internal bool `json:"internal" yaml:"internal"`
	LFS      bool `json:"lfs" yaml:"lfs"`
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

// SECURITY FIX MED-1: Redact password when marshaling to JSON
func (p PostgresConfig) MarshalJSON() ([]byte, error) {
	type Alias PostgresConfig
	return json.Marshal(&struct {
		Password string `json:"password"`
		*Alias
	}{
		Password: "***REDACTED***",
		Alias: (*Alias)(&p),
	})
}

type S3Config struct {
	Bucket    string `json:"bucket" yaml:"bucket"`
	Provider  string `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region    string `json:"region" yaml:"region"`
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key" yaml:"secret_key"`
	Endpoint  string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
}

// SECURITY FIX MED-1: Redact secret key when marshaling to JSON
func (s S3Config) MarshalJSON() ([]byte, error) {
	type Alias S3Config
	return json.Marshal(&struct {
		SecretKey string `json:"secret_key"`
		AccessKey string `json:"access_key"`
		*Alias
	}{
		SecretKey: "***REDACTED***",
		AccessKey: "***REDACTED***",
		Alias: (*Alias)(&s),
	})
}

type AuthConfig struct {
	Mode        string          `json:"mode" yaml:"mode"`
	Basic       BasicAuthConfig `json:"basic" yaml:"basic"`
	Mock        MockAuthConfig  `json:"mock" yaml:"mock"`
	Cache       AuthCacheConfig `json:"cache" yaml:"cache"`
	PluginPaths PluginPaths     `json:"plugin_paths" yaml:"plugin_paths"`
	FenceURL    string          `json:"fence_url" yaml:"fence_url"`
}

type MockAuthConfig struct {
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	RequireAuthHeader bool     `json:"require_auth_header" yaml:"require_auth_header"`
	Resources         []string `json:"resources" yaml:"resources"`
	Methods           []string `json:"methods" yaml:"methods"`
}

type AuthCacheConfig struct {
	Enabled      bool   `json:"enabled" yaml:"enabled"`
	TTLSeconds   int    `json:"ttl_seconds" yaml:"ttl_seconds"`
	NegativeTTL  int    `json:"negative_ttl_seconds" yaml:"negative_ttl_seconds"`
	MaxEntries   int    `json:"max_entries" yaml:"max_entries"`
	CleanupEvery int    `json:"cleanup_seconds" yaml:"cleanup_seconds"`
}

type PluginPaths struct {
	Authz string `json:"authz" yaml:"authz"`
	Authn string `json:"authn" yaml:"authn"`
}

type BasicAuthConfig struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// SECURITY FIX MED-1: Redact password when marshaling to JSON
func (b BasicAuthConfig) MarshalJSON() ([]byte, error) {
	type Alias BasicAuthConfig
	return json.Marshal(&struct {
		Password string `json:"password"`
		*Alias
	}{
		Password: "***REDACTED***",
		Alias: (*Alias)(&b),
	})
}

type SigningConfig struct {
	DefaultExpirySeconds int `json:"default_expiry_seconds" yaml:"default_expiry_seconds"`
}

type LFSConfig struct {
	MaxBatchObjects              int   `json:"max_batch_objects" yaml:"max_batch_objects"`
	MaxBatchBodyBytes            int64 `json:"max_batch_body_bytes" yaml:"max_batch_body_bytes"`
	RequestLimitPerMinute        int   `json:"request_limit_per_minute" yaml:"request_limit_per_minute"`
	BandwidthLimitBytesPerMinute int64 `json:"bandwidth_limit_bytes_per_minute" yaml:"bandwidth_limit_bytes_per_minute"`
}

const (
	DefaultLFSMaxBatchObjects                    = 1000
	DefaultLFSMaxBatchBodyBytes            int64 = 10 * 1024 * 1024 // 10 MiB
	DefaultLFSRequestLimitPerMinute              = 1200             // 20 req/sec average per client
	DefaultLFSBandwidthLimitBytesPerMinute int64 = 0                // disabled by default
)

func LoadConfig(configFile string) (*Config, error) {
	// 1. Default Config
	cfg := &Config{
		Port:     8080,
		Database: DatabaseConfig{},
		Auth:     AuthConfig{},
		LFS: LFSConfig{
			MaxBatchObjects:              DefaultLFSMaxBatchObjects,
			MaxBatchBodyBytes:            DefaultLFSMaxBatchBodyBytes,
			RequestLimitPerMinute:        DefaultLFSRequestLimitPerMinute,
			BandwidthLimitBytesPerMinute: DefaultLFSBandwidthLimitBytesPerMinute,
		},
		Signing: SigningConfig{
			DefaultExpirySeconds: DefaultSigningExpirySeconds,
		},
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
	if mode := os.Getenv("DRS_AUTH_MODE"); mode != "" {
		cfg.Auth.Mode = mode
	}
	if user := os.Getenv("DRS_BASIC_AUTH_USER"); user != "" {
		cfg.Auth.Basic.Username = user
	}
	if pass := os.Getenv("DRS_BASIC_AUTH_PASSWORD"); pass != "" {
		cfg.Auth.Basic.Password = pass
	}
	if v := os.Getenv("DRS_LFS_MAX_BATCH_OBJECTS"); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_LFS_MAX_BATCH_OBJECTS: %s", v)
		}
		cfg.LFS.MaxBatchObjects = i
	}
	if v := os.Getenv("DRS_LFS_MAX_BATCH_BODY_BYTES"); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_LFS_MAX_BATCH_BODY_BYTES: %s", v)
		}
		cfg.LFS.MaxBatchBodyBytes = i
	}
	if v := os.Getenv("DRS_LFS_REQUEST_LIMIT_PER_MINUTE"); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_LFS_REQUEST_LIMIT_PER_MINUTE: %s", v)
		}
		cfg.LFS.RequestLimitPerMinute = i
	}
	if v := os.Getenv("DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE"); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE: %s", v)
		}
		cfg.LFS.BandwidthLimitBytesPerMinute = i
	}
	if v := os.Getenv("DRS_SIGNING_DEFAULT_EXPIRY_SECONDS"); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_SIGNING_DEFAULT_EXPIRY_SECONDS: %s", v)
		}
		cfg.Signing.DefaultExpirySeconds = i
	}
	if v := os.Getenv("DRS_ENABLE_DOCS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_ENABLE_DOCS: %s", v)
		}
		cfg.Routes.Docs = b
	}
	if v := os.Getenv("DRS_ENABLE_GA4GH"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_ENABLE_GA4GH: %s", v)
		}
		cfg.Routes.Ga4gh = b
	}
	if v := os.Getenv("DRS_ENABLE_METRICS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_ENABLE_METRICS: %s", v)
		}
		cfg.Routes.Metrics = b
	}
	if v := os.Getenv("DRS_ENABLE_INTERNAL"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_ENABLE_INTERNAL: %s", v)
		}
		cfg.Routes.Internal = b
	}
	if v := os.Getenv("DRS_ENABLE_LFS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DRS_ENABLE_LFS: %s", v)
		}
		cfg.Routes.LFS = b
	}

	// DB Env Vars overrides
	// If Postgres env vars are provided, we assume Postgres.
	if os.Getenv("DRS_DB_HOST") != "" || os.Getenv("DRS_DB_DATABASE") != "" {
		if cfg.Database.Postgres == nil {
			cfg.Database.Postgres = &PostgresConfig{
				Host:    "localhost",
				Port:    5432,
				SSLMode: "require", // SECURITY FIX MED-2: Default to TLS required
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
			if err != nil {
				return nil, fmt.Errorf("invalid DRS_DB_PORT: %s", v)
			}
			cfg.Database.Postgres.Port = p
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
		bucketProvider, err := common.ParseBucketProvider(cred.Provider)
		if err != nil {
			return nil, fmt.Errorf("s3_credentials[%d]: %w", i, err)
		}
		cfg.S3Credentials[i].Provider = bucketProvider
		if err := common.ValidateBucketName(bucketProvider, cred.Bucket); err != nil {
			return nil, fmt.Errorf("s3_credentials[%d]: %w", i, err)
		}
		if bucketProvider == common.S3Provider {
			if cred.Region == "" {
				return nil, fmt.Errorf("s3_credentials[%d]: region is required for provider=%s", i, bucketProvider)
			}
			if cred.AccessKey == "" {
				return nil, fmt.Errorf("s3_credentials[%d]: access_key is required for provider=%s", i, bucketProvider)
			}
			if cred.SecretKey == "" {
				return nil, fmt.Errorf("s3_credentials[%d]: secret_key is required for provider=%s", i, bucketProvider)
			}
		}
	}
	cfg.Auth.Mode = strings.ToLower(strings.TrimSpace(cfg.Auth.Mode))
	if cfg.Auth.Mode == "" {
		return nil, fmt.Errorf("auth.mode is required and must be one of %q or %q", AuthModeLocal, AuthModeGen3)
	}
	if cfg.Auth.Mode != AuthModeLocal && cfg.Auth.Mode != AuthModeGen3 {
		return nil, fmt.Errorf("invalid auth.mode %q: expected %q or %q", cfg.Auth.Mode, AuthModeLocal, AuthModeGen3)
	}
	if cfg.Auth.Mode == AuthModeGen3 && cfg.Database.Postgres == nil && !isMockAuthEnabledFromEnv() {
		return nil, fmt.Errorf("auth.mode %q requires postgres database", cfg.Auth.Mode)
	}
	if (cfg.Auth.Basic.Username == "") != (cfg.Auth.Basic.Password == "") {
		return nil, fmt.Errorf("both auth.basic.username and auth.basic.password must be set together")
	}

	// SECURITY FIX HIGH-1: Warn if local auth mode is configured without basic auth
	if cfg.Auth.Mode == AuthModeLocal && (cfg.Auth.Basic.Username == "" || cfg.Auth.Basic.Password == "") {
		fmt.Fprintf(os.Stderr, "WARNING: local auth mode configured without basic auth credentials—all endpoints will be unauthenticated and unrestricted. This is only safe for development/testing. Set DRS_BASIC_AUTH_USER and DRS_BASIC_AUTH_PASSWORD to enable basic auth.\n")
	}

	// SECURITY FIX HIGH-2: Mock auth only allowed in local mode
	if isMockAuthEnabledFromEnv() && cfg.Auth.Mode != AuthModeLocal {
		return nil, fmt.Errorf("mock auth (DRS_AUTH_MOCK_ENABLED) is only allowed in local auth mode, not in %q", cfg.Auth.Mode)
	}
	if cfg.LFS.MaxBatchObjects < 0 {
		return nil, fmt.Errorf("lfs.max_batch_objects must be >= 0")
	}
	if cfg.LFS.MaxBatchBodyBytes < 0 {
		return nil, fmt.Errorf("lfs.max_batch_body_bytes must be >= 0")
	}
	if cfg.LFS.RequestLimitPerMinute < 0 {
		return nil, fmt.Errorf("lfs.request_limit_per_minute must be >= 0")
	}
	if cfg.LFS.BandwidthLimitBytesPerMinute < 0 {
		return nil, fmt.Errorf("lfs.bandwidth_limit_bytes_per_minute must be >= 0")
	}

	// 4. Override with Auth.Mock config if set
	if cfg.Auth.Mock.Enabled {
		os.Setenv("DRS_AUTH_MOCK_ENABLED", "true")
	}
	if cfg.Auth.Mock.RequireAuthHeader {
		os.Setenv("DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER", "true")
	}
	if len(cfg.Auth.Mock.Resources) > 0 {
		os.Setenv("DRS_AUTH_MOCK_RESOURCES", strings.Join(cfg.Auth.Mock.Resources, ","))
	}
	if len(cfg.Auth.Mock.Methods) > 0 {
		os.Setenv("DRS_AUTH_MOCK_METHODS", strings.Join(cfg.Auth.Mock.Methods, ","))
	}
	// Auth cache config
	if cfg.Auth.Cache.Enabled {
		os.Setenv("DRS_AUTH_CACHE_ENABLED", "true")
	}
	if cfg.Auth.Cache.TTLSeconds > 0 {
		os.Setenv("DRS_AUTH_CACHE_TTL_SECONDS", strconv.Itoa(cfg.Auth.Cache.TTLSeconds))
	}
	if cfg.Auth.Cache.NegativeTTL > 0 {
		os.Setenv("DRS_AUTH_CACHE_NEGATIVE_TTL_SECONDS", strconv.Itoa(cfg.Auth.Cache.NegativeTTL))
	}
	if cfg.Auth.Cache.MaxEntries > 0 {
		os.Setenv("DRS_AUTH_CACHE_MAX_ENTRIES", strconv.Itoa(cfg.Auth.Cache.MaxEntries))
	}
	if cfg.Auth.Cache.CleanupEvery > 0 {
		os.Setenv("DRS_AUTH_CACHE_CLEANUP_SECONDS", strconv.Itoa(cfg.Auth.Cache.CleanupEvery))
	}
	// Plugin paths
	if cfg.Auth.PluginPaths.Authz != "" {
		os.Setenv("SYFON_AUTHZ_PLUGIN_PATH", cfg.Auth.PluginPaths.Authz)
	}
	if cfg.Auth.PluginPaths.Authn != "" {
		os.Setenv("SYFON_AUTHN_PLUGIN_PATH", cfg.Auth.PluginPaths.Authn)
	}
	// Fence URL
	if cfg.Auth.FenceURL != "" {
		os.Setenv("DRS_FENCE_URL", cfg.Auth.FenceURL)
	}

	return cfg, nil
}

func isMockAuthEnabledFromEnv() bool {
	raw := strings.TrimSpace(os.Getenv("DRS_AUTH_MOCK_ENABLED"))
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
