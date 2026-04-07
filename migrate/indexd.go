// Package migrate implements an API-driven, idempotent ETL pipeline for
// migrating records from an Indexd server to the Syfon DRS data model.
//
// Field mapping (see GitHub issue #20):
//
//	Indexd            → Syfon DRS
//	did               → drs_object.id
//	size              → drs_object.size
//	file_name         → drs_object.name
//	version           → drs_object.version
//	description       → drs_object.description
//	created_date      → drs_object.created_time
//	updated_date      → drs_object.updated_time
//	urls[]            → drs_object_access_method
//	hashes            → drs_object_checksum
//	authz[]           → drs_object_authz
//
// Deprecated fields not migrated: baseid, rev, metadata, urls_metadata, acl,
// form, uploader.
package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// IndexdRecord is the raw record shape returned by Indexd's API.
// Deprecated fields are captured for logging but not migrated.
type IndexdRecord struct {
	DID         string            `json:"did"`
	Size        int64             `json:"size"`
	FileName    string            `json:"file_name"`
	Version     string            `json:"version"`
	Description string            `json:"description"`
	URLs        []string          `json:"urls"`
	Hashes      map[string]string `json:"hashes"`
	Authz       []string          `json:"authz"`
	CreatedDate string            `json:"created_date"`
	UpdatedDate string            `json:"updated_date"`

	// Deprecated – captured but intentionally not migrated.
	Baseid   string   `json:"baseid"`
	Rev      string   `json:"rev"`
	Uploader string   `json:"uploader"`
	ACL      []string `json:"acl"`
	Form     string   `json:"form"`
}

// IndexdPage is the envelope returned by Indexd's list endpoint.
// Indexd may return either a "records" array (syfon-compat) or an "ids" array
// (native indexd) together with a "start" cursor for the next page.
type IndexdPage struct {
	Records []IndexdRecord `json:"records"`
	IDs     []string       `json:"ids"`
	Start   string         `json:"start"`
}

// IndexdClient fetches records from an Indexd (or Syfon-compat) HTTP API.
type IndexdClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewIndexdClient constructs a client pointed at baseURL.
func NewIndexdClient(baseURL string) *IndexdClient {
	return &IndexdClient{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// ListPage fetches one page of records.
//
// It supports both Indexd-native cursor pagination (limit + start DID) and
// Syfon-compat integer page pagination (limit + page).  When start == "" and
// page == 0 the first page is returned.
//
// Returns the records, the next cursor (empty string when exhausted), and any
// error.
func (c *IndexdClient) ListPage(ctx context.Context, limit int, start string, page int) ([]IndexdRecord, string, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET %s: unexpected status %d", endpoint, resp.StatusCode)
	}

	var page_ IndexdPage
	if err := json.NewDecoder(resp.Body).Decode(&page_); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}

	// Native indexd returns an "ids" array; resolve each DID individually.
	if len(page_.Records) == 0 && len(page_.IDs) > 0 {
		records, err := c.fetchByIDs(ctx, page_.IDs)
		if err != nil {
			return nil, "", err
		}
		return records, page_.Start, nil
	}

	return page_.Records, page_.Start, nil
}

// fetchByIDs resolves individual DID records from the native Indexd detail
// endpoint (/index/<did>).
func (c *IndexdClient) fetchByIDs(ctx context.Context, ids []string) ([]IndexdRecord, error) {
	records := make([]IndexdRecord, 0, len(ids))
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
func (c *IndexdClient) GetRecord(ctx context.Context, did string) (*IndexdRecord, error) {
	endpoint := c.baseURL + "/index/" + url.PathEscape(did)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("record not found: %s", did)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: unexpected status %d", endpoint, resp.StatusCode)
	}

	var rec IndexdRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	return &rec, nil
}

