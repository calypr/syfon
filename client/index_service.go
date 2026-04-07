package client

import (
	"context"
	"net/url"
	"strconv"
)

type IndexService struct {
	c *Client
}

func (s *IndexService) Get(ctx context.Context, did string) (InternalRecord, error) {
	var out InternalRecord
	err := s.c.doJSON(ctx, "GET", "/index/"+url.PathEscape(did), nil, nil, &out)
	return out, err
}

func (s *IndexService) Create(ctx context.Context, rec InternalRecord) (InternalRecord, error) {
	var out InternalRecord
	err := s.c.doJSON(ctx, "POST", "/index", nil, rec, &out)
	return out, err
}

func (s *IndexService) Update(ctx context.Context, did string, rec InternalRecord) (InternalRecord, error) {
	var out InternalRecord
	err := s.c.doJSON(ctx, "PUT", "/index/"+url.PathEscape(did), nil, rec, &out)
	return out, err
}

func (s *IndexService) Delete(ctx context.Context, did string) error {
	return s.c.doJSON(ctx, "DELETE", "/index/"+url.PathEscape(did), nil, nil, nil)
}

func (s *IndexService) List(ctx context.Context, opts ListRecordsOptions) (ListRecordsResponse, error) {
	q := url.Values{}
	if opts.Hash != "" {
		q.Set("hash", opts.Hash)
	}
	if opts.Authz != "" {
		q.Set("authz", opts.Authz)
	}
	if opts.Organization != "" {
		q.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set("project", opts.ProjectID)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Page > 0 {
		q.Set("page", strconv.Itoa(opts.Page))
	}
	var out ListRecordsResponse
	err := s.c.doJSON(ctx, "GET", "/index", q, nil, &out)
	return out, err
}

func (s *IndexService) DeleteByQuery(ctx context.Context, opts DeleteByQueryOptions) (DeleteByQueryResponse, error) {
	q := url.Values{}
	if opts.Authz != "" {
		q.Set("authz", opts.Authz)
	}
	if opts.Organization != "" {
		q.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set("project", opts.ProjectID)
	}
	if opts.Hash != "" {
		q.Set("hash", opts.Hash)
	}
	if opts.HashType != "" {
		q.Set("hash_type", opts.HashType)
	}
	var out DeleteByQueryResponse
	err := s.c.doJSON(ctx, "DELETE", "/index", q, nil, &out)
	return out, err
}

func (s *IndexService) BulkCreate(ctx context.Context, req BulkCreateRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	err := s.c.doJSON(ctx, "POST", "/index/bulk", nil, req, &out)
	return out, err
}

func (s *IndexService) BulkHashes(ctx context.Context, req BulkHashesRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	err := s.c.doJSON(ctx, "POST", "/index/bulk/hashes", nil, req, &out)
	return out, err
}

func (s *IndexService) BulkDeleteByHashes(ctx context.Context, req BulkHashesRequest) (DeleteByQueryResponse, error) {
	var out DeleteByQueryResponse
	err := s.c.doJSON(ctx, "POST", "/index/bulk/delete", nil, req, &out)
	return out, err
}

func (s *IndexService) BulkSHA256Validity(ctx context.Context, req BulkSHA256ValidityRequest) (map[string]bool, error) {
	out := map[string]bool{}
	err := s.c.doJSON(ctx, "POST", "/index/bulk/sha256/validity", nil, req, &out)
	return out, err
}

func (s *IndexService) BulkDocuments(ctx context.Context, dids []string) ([]InternalRecord, error) {
	var out []InternalRecord
	err := s.c.doJSON(ctx, "POST", "/index/bulk/documents", nil, dids, &out)
	return out, err
}

// Compatibility wrappers used by current CLI code.
func (c *Client) GetRecord(ctx context.Context, did string) (InternalRecord, error) {
	return c.Index().Get(ctx, did)
}

func (c *Client) PutRecord(ctx context.Context, did string, rec InternalRecord) error {
	_, err := c.Index().Update(ctx, did, rec)
	return err
}

func (c *Client) PostRecord(ctx context.Context, rec InternalRecord) error {
	_, err := c.Index().Create(ctx, rec)
	return err
}
