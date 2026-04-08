package migrate

import "context"

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

// SourceLister fetches pages of records from an Indexd-compatible source.
type SourceLister interface {
	ListPage(ctx context.Context, limit int, start string, page int) ([]IndexdRecord, string, error)
}

