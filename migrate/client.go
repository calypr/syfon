package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxHTTPAttempts = 3

type BasicAuth struct {
	Username string
	Password string
}

type AuthConfig struct {
	BearerToken string
	Basic       *BasicAuth
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	auth       AuthConfig
	userAgent  string
}

func NewHTTPClient(baseURL string, auth AuthConfig, httpClient *http.Client) (*HTTPClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("base URL is required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &HTTPClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		auth:       auth,
		userAgent:  "syfon-migrate",
	}, nil
}

func (c *HTTPClient) ListPage(ctx context.Context, limit int, start string) ([]IndexdRecord, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	if strings.TrimSpace(start) != "" {
		q.Set("start", start)
	}
	endpoint := c.endpoint("/index")
	if encoded := q.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.applyHeaders(req)
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s: unexpected status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var page IndexdPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decode %s: %w", endpoint, err)
	}
	if len(page.Records) > 0 {
		return page.Records, nil
	}
	if len(page.IDs) > 0 {
		return c.fetchDocuments(ctx, page.IDs)
	}
	return nil, nil
}

func (c *HTTPClient) fetchDocuments(ctx context.Context, ids []string) ([]IndexdRecord, error) {
	records, err := c.bulkDocuments(ctx, ids)
	if err == nil {
		return records, nil
	}

	out := make([]IndexdRecord, 0, len(ids))
	for _, id := range ids {
		rec, getErr := c.getRecord(ctx, id)
		if getErr != nil {
			return nil, fmt.Errorf("bulk documents failed: %v; detail fetch %s failed: %w", err, id, getErr)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (c *HTTPClient) bulkDocuments(ctx context.Context, ids []string) ([]IndexdRecord, error) {
	body, err := json.Marshal(ids)
	if err != nil {
		return nil, err
	}
	endpoint := c.endpoint("/index/bulk/documents")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.applyHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("POST %s: unexpected status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var records []IndexdRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, fmt.Errorf("decode %s: %w", endpoint, err)
	}
	return records, nil
}

func (c *HTTPClient) getRecord(ctx context.Context, id string) (IndexdRecord, error) {
	endpoint := c.endpoint("/index/") + url.PathEscape(id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return IndexdRecord{}, err
	}
	c.applyHeaders(req)
	resp, err := c.do(req)
	if err != nil {
		return IndexdRecord{}, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return IndexdRecord{}, fmt.Errorf("GET %s: unexpected status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var record IndexdRecord
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		return IndexdRecord{}, fmt.Errorf("decode %s: %w", endpoint, err)
	}
	return record, nil
}

func (c *HTTPClient) LoadBatch(ctx context.Context, records []MigrationRecord) error {
	body, err := json.Marshal(bulkCreateRequestFromMigration(records))
	if err != nil {
		return err
	}
	endpoint := c.endpoint("/index/bulk")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.applyHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: unexpected status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

type bulkCreateRequest struct {
	Records []bulkInternalRecord `json:"records"`
}

type bulkInternalRecord struct {
	Auth        *map[string]map[string][]string `json:"auth,omitempty"`
	CreatedTime *string                         `json:"created_time,omitempty"`
	Description *string                         `json:"description,omitempty"`
	Did         string                          `json:"did"`
	FileName    *string                         `json:"file_name,omitempty"`
	Hashes      *map[string]string              `json:"hashes,omitempty"`
	Size        *int64                          `json:"size,omitempty"`
	UpdatedTime *string                         `json:"updated_time,omitempty"`
	Version     *string                         `json:"version,omitempty"`
}

func bulkCreateRequestFromMigration(records []MigrationRecord) bulkCreateRequest {
	out := bulkCreateRequest{Records: make([]bulkInternalRecord, 0, len(records))}
	for _, record := range records {
		size := record.Size
		created := record.CreatedTime.Format(time.RFC3339Nano)
		var updated *string
		if record.UpdatedTime != nil {
			value := record.UpdatedTime.Format(time.RFC3339Nano)
			updated = &value
		}
		hashes := make(map[string]string, len(record.Checksums))
		for _, checksum := range record.Checksums {
			if checksum.Type != "" && checksum.Checksum != "" {
				hashes[checksum.Type] = checksum.Checksum
			}
		}
		auth := authPathMapFromMigration(record)
		out.Records = append(out.Records, bulkInternalRecord{
			Did:         record.ID,
			Size:        &size,
			FileName:    record.Name,
			Version:     record.Version,
			Description: record.Description,
			CreatedTime: &created,
			UpdatedTime: updated,
			Hashes:      &hashes,
			Auth:        auth,
		})
	}
	return out
}

func authPathMapFromMigration(record MigrationRecord) *map[string]map[string][]string {
	if len(record.AccessMethods) == 0 || len(record.Authz) == 0 {
		return nil
	}
	auth := make(map[string]map[string][]string)
	for _, resource := range record.Authz {
		org, project := parseResourcePath(resource)
		if org == "" {
			continue
		}
		if auth[org] == nil {
			auth[org] = make(map[string][]string)
		}
		for _, method := range record.AccessMethods {
			if method.AccessUrl == nil || method.AccessUrl.Url == "" {
				continue
			}
			auth[org][project] = appendUnique(auth[org][project], method.AccessUrl.Url)
		}
	}
	if len(auth) == 0 {
		return nil
	}
	return &auth
}

func parseResourcePath(path string) (string, string) {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "/programs/")
	parts := strings.SplitN(path, "/projects/", 2)
	if len(parts) == 1 {
		return strings.TrimSpace(parts[0]), ""
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (c *HTTPClient) endpoint(path string) string {
	path = "/" + strings.TrimLeft(path, "/")
	if strings.HasSuffix(c.baseURL, "/index") {
		if path == "/index" {
			return c.baseURL
		}
		return c.baseURL + strings.TrimPrefix(path, "/index")
	}
	return c.baseURL + path
}

func (c *HTTPClient) do(req *http.Request) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= maxHTTPAttempts; attempt++ {
		if attempt > 1 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			req.Body = body
		}

		resp, err := c.httpClient.Do(req)
		if !shouldRetryHTTP(resp, err) || attempt == maxHTTPAttempts {
			return resp, err
		}
		if err != nil {
			lastErr = err
		}
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}

		timer := time.NewTimer(time.Duration(attempt) * 750 * time.Millisecond)
		select {
		case <-req.Context().Done():
			timer.Stop()
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, req.Context().Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func shouldRetryHTTP(resp *http.Response, err error) bool {
	if err != nil {
		var netErr net.Error
		return errors.As(err, &netErr) && netErr.Timeout()
	}
	if resp == nil {
		return false
	}
	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError
}

func (c *HTTPClient) applyHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if strings.TrimSpace(c.auth.BearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.auth.BearerToken))
		return
	}
	if c.auth.Basic != nil && c.auth.Basic.Username != "" {
		req.SetBasicAuth(c.auth.Basic.Username, c.auth.Basic.Password)
	}
}
