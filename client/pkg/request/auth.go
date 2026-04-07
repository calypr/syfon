package request

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/conf"
)

func (t *AuthTransport) NewAccessToken(ctx context.Context) error {
	if t.Cred.APIKey == "" {
		return errors.New("APIKey is required to refresh access token")
	}

	refreshClient := &http.Client{Transport: t.Base}

	payload := map[string]string{"api_key": t.Cred.APIKey}
	reader, err := common.ToJSONReader(payload)
	if err != nil {
		return err
	}

	refreshUrl := t.Cred.APIEndpoint + common.FenceAccessTokenEndpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshUrl, reader)
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
		return errors.New("failed to refresh token, status: " + strconv.Itoa(resp.StatusCode))
	}

	var result common.AccessTokenStruct
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	t.mu.Lock()
	t.Cred.AccessToken = result.AccessToken
	if t.Manager != nil {
		t.Manager.Save(t.Cred)
	}
	t.mu.Unlock()
	return nil
}

type AuthTransport struct {
	Manager   conf.ManagerInterface
	Base      http.RoundTripper
	Cred      *conf.Credential
	mu        sync.RWMutex
	refreshMu sync.Mutex
}

func (t *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("X-Skip-Auth") == "true" {
		req.Header.Del("X-Skip-Auth")
		return t.Base.RoundTrip(req)
	}

	t.mu.RLock()
	token := t.Cred.AccessToken
	t.mu.RUnlock()

	// Just add the header and pass it down
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return t.Base.RoundTrip(req)
}

func (t *AuthTransport) refreshOnce(ctx context.Context) error {
	t.refreshMu.Lock()
	defer t.refreshMu.Unlock()

	t.mu.RLock()
	if t.Cred.AccessToken != "" {
		t.mu.RUnlock()
		return nil
	}
	t.mu.RUnlock()

	return t.NewAccessToken(ctx)
}
