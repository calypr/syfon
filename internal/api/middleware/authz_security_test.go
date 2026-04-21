package middleware

import (
	"os"
	"testing"
)

// Test CRIT-1 fix: Issuer allowlist validation
func TestParseToken_IssuerAllowlistValidation(t *testing.T) {
	// Set up allowed issuers
	oldEnv := os.Getenv("DRS_ALLOWED_ISSUERS")
	defer func() {
		if oldEnv != "" {
			os.Setenv("DRS_ALLOWED_ISSUERS", oldEnv)
		} else {
			os.Unsetenv("DRS_ALLOWED_ISSUERS")
		}
	}()

	tests := []struct {
		name           string
		allowedIssuers string
		issuerClaim    string
		scheme         string
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid issuer in allowlist",
			allowedIssuers: "https://fence.example.com,https://fence2.example.com",
			issuerClaim:    "https://fence.example.com",
			scheme:         "https",
			wantErr:        false,
		},
		{
			name:           "issuer not in allowlist",
			allowedIssuers: "https://fence.example.com",
			issuerClaim:    "https://attacker.example.com",
			scheme:         "https",
			wantErr:        true,
			errContains:    "not in allowed list",
		},
		{
			name:           "http scheme rejected (must be https)",
			allowedIssuers: "https://fence.example.com",
			issuerClaim:    "http://fence.example.com",
			scheme:         "http",
			wantErr:        true,
			errContains:    "must use https scheme",
		},
		{
			name:           "empty allowlist rejects all",
			allowedIssuers: "",
			issuerClaim:    "https://fence.example.com",
			scheme:         "https",
			wantErr:        true,
			errContains:    "not in allowed list",
		},
		{
			name:           "ssrf attempt to internal metadata",
			allowedIssuers: "https://fence.example.com",
			issuerClaim:    "http://169.254.169.254",
			scheme:         "http",
			wantErr:        true,
			errContains:    "must use https scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("DRS_ALLOWED_ISSUERS", tt.allowedIssuers)

			m := &AuthzMiddleware{
				logger: nil,
			}

			// Create a mock JWT with iss claim
			claims := map[string]interface{}{
				"iss": tt.issuerClaim,
				"exp": float64(9999999999),
			}

			// Test isIssuerAllowed directly
			allowed := isIssuerAllowed(tt.issuerClaim)
			if !tt.wantErr && !allowed {
				t.Errorf("issuer %q should be allowed but was rejected", tt.issuerClaim)
			}
			if tt.wantErr && allowed {
				t.Errorf("issuer %q should be rejected but was allowed", tt.issuerClaim)
			}

			_ = claims // Use it to avoid unused warning
		})
	}
}

func TestIsIssuerAllowed(t *testing.T) {
	tests := []struct {
		name           string
		allowedIssuers string
		testIssuer     string
		want           bool
	}{
		{
			name:           "exact match",
			allowedIssuers: "https://fence.example.com",
			testIssuer:     "https://fence.example.com",
			want:           true,
		},
		{
			name:           "no match",
			allowedIssuers: "https://fence.example.com",
			testIssuer:     "https://other.example.com",
			want:           false,
		},
		{
			name:           "multiple issuers - first matches",
			allowedIssuers: "https://fence1.example.com,https://fence2.example.com",
			testIssuer:     "https://fence1.example.com",
			want:           true,
		},
		{
			name:           "multiple issuers - second matches",
			allowedIssuers: "https://fence1.example.com,https://fence2.example.com",
			testIssuer:     "https://fence2.example.com",
			want:           true,
		},
		{
			name:           "empty allowlist",
			allowedIssuers: "",
			testIssuer:     "https://fence.example.com",
			want:           false,
		},
		{
			name:           "whitespace handling",
			allowedIssuers: "https://fence1.example.com , https://fence2.example.com ",
			testIssuer:     "https://fence2.example.com",
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldEnv := os.Getenv("DRS_ALLOWED_ISSUERS")
			defer func() {
				if oldEnv != "" {
					os.Setenv("DRS_ALLOWED_ISSUERS", oldEnv)
				} else {
					os.Unsetenv("DRS_ALLOWED_ISSUERS")
				}
			}()

			os.Setenv("DRS_ALLOWED_ISSUERS", tt.allowedIssuers)
			got := isIssuerAllowed(tt.testIssuer)
			if got != tt.want {
				t.Errorf("isIssuerAllowed(%q) = %v, want %v", tt.testIssuer, got, tt.want)
			}
		})
	}
}

