package client

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
)

type IndexService struct {
	base *baseService
}

func (s *IndexService) Get(ctx context.Context, did string) (InternalRecord, error) {
	var out InternalRecord
	rb := s.base.requestor.New("GET", "/index/"+url.PathEscape(did))
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) Create(ctx context.Context, rec InternalRecord) (InternalRecord, error) {
	var out InternalRecord
	rb, err := s.base.requestor.New("POST", "/index").WithJSONBody(rec)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) Update(ctx context.Context, did string, rec InternalRecord) (InternalRecord, error) {
	var out InternalRecord
	rb, err := s.base.requestor.New("PUT", "/index/"+url.PathEscape(did)).WithJSONBody(rec)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) Delete(ctx context.Context, did string) error {
	rb := s.base.requestor.New("DELETE", "/index/"+url.PathEscape(did))
	return s.base.requestor.DoJSON(ctx, rb, nil)
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
	rb := s.base.requestor.New("GET", "/index").WithQueryValues(q)
	err := s.base.requestor.DoJSON(ctx, rb, &out)
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
	rb := s.base.requestor.New("DELETE", "/index").WithQueryValues(q)
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) BulkCreate(ctx context.Context, req BulkCreateRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	rb, err := s.base.requestor.New("POST", "/index/bulk").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) BulkHashes(ctx context.Context, req BulkHashesRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	rb, err := s.base.requestor.New("POST", "/index/bulk/hashes").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) BulkDeleteByHashes(ctx context.Context, req BulkHashesRequest) (DeleteByQueryResponse, error) {
	var out DeleteByQueryResponse
	rb, err := s.base.requestor.New("POST", "/index/bulk/delete").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) BulkSHA256Validity(ctx context.Context, req BulkSHA256ValidityRequest) (map[string]bool, error) {
	out := map[string]bool{}
	rb, err := s.base.requestor.New("POST", "/index/bulk/sha256/validity").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) BulkDocuments(ctx context.Context, dids []string) ([]InternalRecord, error) {
	var out []InternalRecord
	rb, err := s.base.requestor.New("POST", "/index/bulk/documents").WithJSONBody(dids)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) SHA256Validity(ctx context.Context, values []string) (map[string]bool, error) {
	payload := map[string][]string{"sha256": values}
	out := map[string]bool{}
	rb, err := s.base.requestor.New("POST", "/index/v1/sha256/validity").WithJSONBody(payload)
	if err != nil {
		return out, err
	}
	err = s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *IndexService) Upsert(ctx context.Context, did, objectURL, fileName string, size int64, sha256sum string, authz []string) error {
	existing, err := s.Get(ctx, did)
	if err == nil {
		if strings.TrimSpace(existing.Did) == "" {
			existing.Did = did
		}
		if len(existing.Authz) == 0 {
			if len(authz) == 0 {
				return fmt.Errorf("authz is required to upsert record %s", did)
			}
			existing.Authz = append([]string(nil), authz...)
		}
		if fileName != "" {
			existing.FileName = &fileName
		}
		if size > 0 {
			existing.Size = &size
		}
		if objectURL != "" {
			var urls []string
			if existing.Urls != nil {
				urls = *existing.Urls
			}
			seen := map[string]bool{}
			for _, u := range urls {
				seen[u] = true
			}
			if !seen[objectURL] {
				urls = append(urls, objectURL)
				existing.Urls = &urls
			}
		}
		if sha256sum != "" {
			if existing.Hashes == nil {
				h := make(internalapi.HashInfo)
				existing.Hashes = &h
			}
			(*existing.Hashes)["sha256"] = sha256sum
		}
		_, err := s.Update(ctx, did, existing)
		return err
	}

	payload := InternalRecord{
		Did: did,
	}
	if len(authz) == 0 {
		return fmt.Errorf("authz is required to create record %s", did)
	}
	payload.Authz = append([]string(nil), authz...)
	if size > 0 {
		payload.Size = &size
	}
	if objectURL != "" {
		u := []string{objectURL}
		payload.Urls = &u
	}
	if fileName != "" {
		payload.FileName = &fileName
	}
	if sha256sum != "" {
		h := internalapi.HashInfo{"sha256": sha256sum}
		payload.Hashes = &h
	}
	_, err = s.Create(ctx, payload)
	return err
}

// --- IndexService ---
