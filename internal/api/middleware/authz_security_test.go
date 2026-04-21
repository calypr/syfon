package middleware

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// Test CRIT-1 fix: Issuer allowlist validation
func TestParseToken_IssuerAllowlistValidation(t *testing.T) {
	oldTransport := http.DefaultTransport
	http.DefaultTransport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer func() { http.DefaultTransport = oldTransport }()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}

	kid := "test-kid"
	jwksBody := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "RSA",
				"use": "sig",
				"kid": kid,
				"n":   base64.RawURLEncoding.EncodeToString(privKey.PublicKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(privKey.PublicKey.E)).Bytes()),
			},
		},
	}

	httpsJWKS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user/jwt/keys" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(jwksBody)
	}))
	defer httpsJWKS.Close()

	httpJWKS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user/jwt/keys" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(jwksBody)
	}))
	defer httpJWKS.Close()

	buildTokenWithClaims := func(claims jwt.MapClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = kid
		s, signErr := tok.SignedString(privKey)
		if signErr != nil {
			t.Fatalf("sign token with claims: %v", signErr)
		}
		return s
	}

	httpsOrigin := httpsJWKS.URL
	httpOrigin := httpJWKS.URL

	tests := []struct {
		name         string
		issuerClaim  string
		expUnix      int64
		wantErr      bool
		errContains  string
		wantEndpoint string
	}{
		{
			name:         "valid https issuer with path — endpoint is normalized origin",
			issuerClaim:  httpsOrigin + "/user",
			expUnix:      1893456000,
			wantEndpoint: httpsOrigin,
		},
		{
			name:        "http issuer rejected — must use HTTPS",
			issuerClaim: httpOrigin + "/user",
			expUnix:     1893456000,
			wantErr:     true,
			errContains: "must use HTTPS",
		},
	}

	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenString := buildTokenWithClaims(jwt.MapClaims{
				"iss": tt.issuerClaim,
				"exp": tt.expUnix,
			})
			endpoint, exp, parseErr := m.parseToken(tokenString)

			if tt.wantErr {
				if parseErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(parseErr.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", parseErr.Error(), tt.errContains)
				}
				return
			}

			if parseErr != nil {
				t.Fatalf("unexpected parseToken error: %v", parseErr)
			}
			if endpoint != tt.wantEndpoint {
				t.Fatalf("endpoint = %q, want %q", endpoint, tt.wantEndpoint)
			}
			if exp != float64(tt.expUnix) {
				t.Fatalf("exp = %v, want %v", exp, float64(tt.expUnix))
			}
		})
	}

	t.Run("malformed iss claim rejects token", func(t *testing.T) {
		tokenString := buildTokenWithClaims(jwt.MapClaims{
			"iss": 12345,
			"exp": int64(1893456000),
		})

		endpoint, exp, parseErr := m.parseToken(tokenString)
		if parseErr == nil {
			t.Fatalf("expected error for malformed iss, got nil")
		}
		if !strings.Contains(parseErr.Error(), "missing or invalid 'iss' claim") {
			t.Fatalf("error %q does not contain malformed iss message", parseErr.Error())
		}
		if endpoint != "" {
			t.Fatalf("endpoint = %q, want empty", endpoint)
		}
		if exp != 0 {
			t.Fatalf("exp = %v, want 0", exp)
		}
	})
}

func TestNormalizeIssuerOrigin(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "bare origin", input: "https://fence.example.com", want: "https://fence.example.com"},
		{name: "path stripped", input: "https://fence.example.com/user", want: "https://fence.example.com"},
		{name: "path and query stripped", input: "https://fence.example.com/user?x=1#frag", want: "https://fence.example.com"},
		{name: "trailing slash stripped", input: "https://fence.example.com/", want: "https://fence.example.com"},
		{name: "host lowercased", input: "https://FENCE.EXAMPLE.COM", want: "https://fence.example.com"},
		{name: "scheme lowercased", input: "HTTPS://fence.example.com", want: "https://fence.example.com"},
		{name: "missing scheme errors", input: "fence.example.com", wantErr: true},
		{name: "missing host errors", input: "https://", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeIssuerOrigin(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("normalizeIssuerOrigin(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

