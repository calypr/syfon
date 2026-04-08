package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/calypr/data-client/conf"
	"github.com/calypr/data-client/fence"
	"github.com/calypr/data-client/logs"
	"github.com/calypr/data-client/request"
	"github.com/calypr/syfon/migrate"
)

// IndexdClient fetches records from an Indexd (or Syfon-compat) HTTP API
// using a Gen3-authenticated request client.
type IndexdClient struct {
	baseURL  string
	g3client request.RequestInterface
}

// NewIndexdClient constructs a client pointed at baseURL.
// If profile is non-empty, credentials are loaded from ~/.gen3/gen3_client_config.ini.
func NewIndexdClient(baseURL, profile string) (*IndexdClient, error) {
	profile = strings.TrimSpace(profile)
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")

	logger := slog.Default()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	manager := conf.NewConfigure(logger)

	cred := &conf.Credential{}
	if profile != "" {
		loaded, err := manager.Load(profile)
		if err != nil {
			return nil, fmt.Errorf("load gen3 profile %q: %w", profile, err)
		}
		cred = loaded
	}
	err := EnsureValidCredential(context.Background(), manager, cred, logger) // Ensure config file exists; no-op if already valid.
	if err != nil {
		return nil, err
	}

	gen3Logger := logs.NewGen3Logger(logger, "", profile)
	g3client := request.NewRequestInterface(gen3Logger, cred, manager)

	return &IndexdClient{
		baseURL:  baseURL,
		g3client: g3client,
	}, nil
}

// ListPage fetches one page of records.
//
// Supports both cursor-based (start DID) and page-number pagination.
// Returns records, next cursor, and any error.
func (c *IndexdClient) ListPage(ctx context.Context, limit int, start string, page int) ([]migrate.IndexdRecord, string, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	if start != "" {
		q.Set("start", start)
	} else if page > 0 {
		q.Set("page", fmt.Sprintf("%d", page))
	}

	endpoint := c.baseURL + "/index?" + q.Encode()
	rb := c.g3client.New(http.MethodGet, endpoint).
		WithHeader("Accept", "application/json")
	resp, err := c.g3client.Do(ctx, rb)
	if err != nil {
		return nil, "", fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseText, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("GET %s: unexpected status %d body %s", endpoint, resp.StatusCode, responseText)
	}

	var pg migrate.IndexdPage
	if err := json.NewDecoder(resp.Body).Decode(&pg); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}

	// Native indexd returns an "ids" array; resolve each DID individually.
	if len(pg.Records) == 0 && len(pg.IDs) > 0 {
		records, err := c.fetchByIDs(ctx, pg.IDs)
		if err != nil {
			return nil, "", err
		}
		return records, pg.Start, nil
	}

	return pg.Records, pg.Start, nil
}

// fetchByIDs resolves individual DID records from the native Indexd detail
// endpoint (/index/<did>).
func (c *IndexdClient) fetchByIDs(ctx context.Context, ids []string) ([]migrate.IndexdRecord, error) {
	records := make([]migrate.IndexdRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := c.GetRecord(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", id, err)
		}
		records = append(records, *rec)
	}
	return records, nil
}

// GetRecord fetches a single IndexdRecord by DID.
func (c *IndexdClient) GetRecord(ctx context.Context, did string) (*migrate.IndexdRecord, error) {
	endpoint := c.baseURL + "/index/" + url.PathEscape(did)

	rb := c.g3client.New(http.MethodGet, endpoint).
		WithHeader("Accept", "application/json")
	resp, err := c.g3client.Do(ctx, rb)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("record not found: %s", did)
	}
	if resp.StatusCode != http.StatusOK {
		responseText, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s: unexpected status %d body %s", endpoint, resp.StatusCode, responseText)
	}

	var rec migrate.IndexdRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	return &rec, nil
}

// EnsureValidCredential validates a profile credential and refreshes access token
// from API key when possible.
func EnsureValidCredential(ctx context.Context, manager conf.ManagerInterface, cred *conf.Credential, baseLogger *slog.Logger) error {
	logger := logs.NewGen3Logger(baseLogger, "", cred.Profile)

	valid, err := manager.IsCredentialValid(cred)
	if valid {
		return nil
	}
	if err == nil {
		return fmt.Errorf("invalid credential")
	}

	// Keep legacy behavior: only auto-refresh when API key is valid but token expired.
	if !strings.Contains(err.Error(), "access_token is invalid but api_key is valid") {
		return fmt.Errorf("invalid credential: %v", err)
	}

	req := request.NewRequestInterface(logger, cred, manager)
	fClient := fence.NewFenceClient(req, cred, baseLogger)
	newToken, refreshErr := fClient.NewAccessToken(ctx)
	if refreshErr != nil {
		return fmt.Errorf("failed to refresh access token: %v (original error: %v)", refreshErr, err)
	}

	cred.AccessToken = newToken
	if saveErr := manager.Save(cred); saveErr != nil {
		logger.Warn(fmt.Sprintf("failed to save refreshed token: %v", saveErr))
	}
	return nil
}
