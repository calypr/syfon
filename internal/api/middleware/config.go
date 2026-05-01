package middleware

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func loadAuthCacheConfigFromEnv() authCacheConfig {
	cfg := authCacheConfig{
		Enabled:      parseBoolEnv("DRS_AUTH_CACHE_ENABLED", true),
		TTL:          parseDurationSecondsEnv("DRS_AUTH_CACHE_TTL_SECONDS", 45),
		NegativeTTL:  parseDurationSecondsEnv("DRS_AUTH_CACHE_NEGATIVE_TTL_SECONDS", 8),
		MaxEntries:   parseIntEnv("DRS_AUTH_CACHE_MAX_ENTRIES", 20000),
		CleanupEvery: parseDurationSecondsEnv("DRS_AUTH_CACHE_CLEANUP_SECONDS", 60),
	}
	if cfg.MaxEntries < 1 {
		cfg.MaxEntries = 1
	}
	return cfg
}

func parseDurationSecondsEnv(name string, defSeconds int) time.Duration {
	v := parseIntEnv(name, defSeconds)
	if v < 0 {
		v = defSeconds
	}
	return time.Duration(v) * time.Second
}

func parseIntEnv(name string, def int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	var v int
	_, err := fmt.Sscanf(raw, "%d", &v)
	if err != nil {
		return def
	}
	return v
}

func loadMockAuthConfigFromEnv() mockAuthConfig {
	enabled := parseBoolEnv("DRS_AUTH_MOCK_ENABLED", false)
	if !enabled {
		return mockAuthConfig{}
	}
	resources := splitCSV(os.Getenv("DRS_AUTH_MOCK_RESOURCES"))
	if len(resources) == 0 {
		resources = []string{"/data_file"}
	}
	methods := splitCSV(os.Getenv("DRS_AUTH_MOCK_METHODS"))
	if len(methods) == 0 {
		methods = []string{"*"}
	}
	return mockAuthConfig{
		Enabled:           true,
		RequireAuthHeader: parseBoolEnv("DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER", false),
		Resources:         resources,
		Methods:           methods,
	}
}

func parseBoolEnv(name string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func splitCSV(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
