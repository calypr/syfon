package request

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/client/common"
	conf "github.com/calypr/syfon/client/config"
)

type AuthMode string

const (
	AuthModeBasic  AuthMode = "basic"
	AuthModeBearer AuthMode = "bearer"
	SkipAuthHeader          = "X-Skip-Auth"
)

type accessTokenResponse struct {
	AccessToken string `json:"access_token"`
}

type authRequestContextKey struct{}

type authRequestContext struct {
	skipAuth     bool
	explicitAuth bool
}

func SkipAuth(req *http.Request) {
	if req != nil {
		req.Header.Set(SkipAuthHeader, "true")
	}
}

func (t *AuthTransport) NewAccessToken(ctx context.Context) error {
	if t.Mode != AuthModeBearer {
		return nil
	}
	t.mu.RLock()
	cred := t.Cred
	apiKey := ""
	apiEndpoint := ""
	if cred != nil {
		apiKey = cred.APIKey
		apiEndpoint = strings.TrimSpace(cred.APIEndpoint)
	}
	t.mu.RUnlock()
	if cred == nil || apiKey == "" {
		return fmt.Errorf("APIKey is required to refresh access token")
	}
	if apiEndpoint == "" {
		return fmt.Errorf("APIEndpoint is required to refresh access token")
	}

	payload, err := json.Marshal(map[string]string{"api_key": apiKey})
	if err != nil {
		return fmt.Errorf("encode token refresh request: %w", err)
	}

	refreshUrl := strings.TrimRight(apiEndpoint, "/") + common.DataAccessTokenEndpoint
	refreshClient := &http.Client{
		Transport: t.Base,
		Timeout:   t.refreshTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !sameHTTPOrigin(refreshUrl, req.URL) {
				return http.ErrUseLastResponse
			}
			if t.refreshCheckRedirect != nil {
				return t.refreshCheckRedirect(req, via)
			}
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshUrl, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set(common.HeaderContentType, common.MIMEApplicationJSON)

	resp, err := refreshClient.Do(req)
	if err != nil {
		return fmt.Errorf("refresh request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return fmt.Errorf("failed to refresh token: read error response body: %w", err)
		}
		bodyText := strings.TrimSpace(string(body))
		if bodyText == "" {
			return fmt.Errorf("failed to refresh token: %s", resp.Status)
		}
		return fmt.Errorf("failed to refresh token: %s body=%s", resp.Status, bodyText)
	}

	var result accessTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if strings.TrimSpace(result.AccessToken) == "" {
		return fmt.Errorf("refresh response missing access_token")
	}

	t.mu.Lock()
	previousToken := t.Cred.AccessToken
	t.Cred.AccessToken = strings.TrimSpace(result.AccessToken)
	if t.Manager != nil {
		if err := t.Manager.Save(t.Cred); err != nil {
			t.Cred.AccessToken = previousToken
			t.mu.Unlock()
			return fmt.Errorf("save refreshed access token: %w", err)
		}
	}
	t.mu.Unlock()
	return nil
}

type AuthTransport struct {
	Manager              conf.ManagerInterface
	Base                 http.RoundTripper
	Cred                 *conf.Credential
	Mode                 AuthMode
	refreshTimeout       time.Duration
	refreshCheckRedirect func(*http.Request, []*http.Request) error
	mu                   sync.RWMutex
	refreshMu            sync.Mutex
}

func (t *AuthTransport) apply(req *http.Request) {
	skipAuth := req.Header.Get(SkipAuthHeader) == "true"
	if skipAuth {
		req.Header.Del(SkipAuthHeader)
		return
	}
	if req.Header.Get("Authorization") != "" {
		return
	}
	if t.Cred == nil {
		return
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	if !sameHTTPOrigin(t.Cred.APIEndpoint, req.URL) {
		return
	}

	switch t.Mode {
	case AuthModeBearer:
		if token := strings.TrimSpace(t.Cred.AccessToken); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case AuthModeBasic:
		if user := strings.TrimSpace(t.Cred.KeyID); user != "" {
			req.SetBasicAuth(user, t.Cred.APIKey)
		}
	}
}

func sameHTTPOrigin(apiEndpoint string, target *url.URL) bool {
	trusted, err := url.Parse(strings.TrimSpace(apiEndpoint))
	if err != nil {
		return false
	}
	trustedScheme, trustedHost, trustedPort, trustedOK := httpOrigin(trusted)
	targetScheme, targetHost, targetPort, targetOK := httpOrigin(target)
	return trustedOK && targetOK && trustedScheme == targetScheme && trustedHost == targetHost && trustedPort == targetPort
}

func (t *AuthTransport) isTrustedTarget(target *url.URL) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Cred != nil && sameHTTPOrigin(t.Cred.APIEndpoint, target)
}

func httpOrigin(u *url.URL) (scheme, host, port string, ok bool) {
	if u == nil || u.Opaque != "" || u.User != nil {
		return "", "", "", false
	}
	scheme = strings.ToLower(u.Scheme)
	switch scheme {
	case "http":
		port = "80"
	case "https":
		port = "443"
	default:
		return "", "", "", false
	}
	host = strings.ToLower(u.Hostname())
	if host == "" {
		return "", "", "", false
	}
	if explicitPort := u.Port(); explicitPort != "" {
		n, err := strconv.Atoi(explicitPort)
		if err != nil || n < 0 || n > 65535 {
			return "", "", "", false
		}
		port = strconv.Itoa(n)
	}
	return scheme, host, port, true
}

func (t *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	clone := req.Clone(req.Context())
	clone = clone.WithContext(context.WithValue(clone.Context(), authRequestContextKey{}, authRequestContext{
		skipAuth:     req.Header.Get(SkipAuthHeader) == "true",
		explicitAuth: req.Header.Get("Authorization") != "",
	}))
	t.apply(clone)
	return t.Base.RoundTrip(clone)
}

func (t *AuthTransport) refreshIfCurrent(ctx context.Context, rejectedToken string) error {
	if t.Mode != AuthModeBearer {
		return nil
	}
	t.refreshMu.Lock()
	defer t.refreshMu.Unlock()

	t.mu.RLock()
	if t.Cred == nil {
		t.mu.RUnlock()
		return nil
	}
	currentToken := strings.TrimSpace(t.Cred.AccessToken)
	apiEndpoint := strings.TrimSpace(t.Cred.APIEndpoint)
	t.mu.RUnlock()
	if apiEndpoint == "" || currentToken != rejectedToken {
		return nil
	}

	return t.NewAccessToken(ctx)
}
