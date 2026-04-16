package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type IndexService struct {
	requestor request.Requester
}

func NewIndexService(r request.Requester) *IndexService {
	return &IndexService{requestor: r}
}

func (s *IndexService) Get(ctx context.Context, did string) (InternalRecordResponse, error) {
	var out InternalRecordResponse
	err := s.requestor.Do(ctx, http.MethodGet, fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(did)), nil, &out)
	return out, err
}

func (s *IndexService) Create(ctx context.Context, rec InternalRecordRequest) (InternalRecordResponse, error) {
	var out InternalRecordResponse
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexEndpoint, rec, &out)
	return out, err
}

func (s *IndexService) Update(ctx context.Context, did string, rec InternalRecordRequest) (InternalRecordResponse, error) {
	var out InternalRecordResponse
	err := s.requestor.Do(ctx, http.MethodPut, fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(did)), rec, &out)
	return out, err
}

func (s *IndexService) Delete(ctx context.Context, did string) error {
	return s.requestor.Do(ctx, http.MethodDelete, fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(did)), nil, nil)
}

func (s *IndexService) List(ctx context.Context, opts ListRecordsOptions) (ListRecordsResponse, error) {
	q := url.Values{}
	if opts.Hash != "" {
		q.Set(common.QueryParamHash, opts.Hash)
	}
	if opts.Authz != "" {
		q.Set(common.QueryParamAuthz, opts.Authz)
	}
	if opts.Organization != "" {
		q.Set(common.QueryParamOrganization, opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set(common.QueryParamProject, opts.ProjectID)
	}
	if opts.Limit > 0 {
		q.Set(common.QueryParamLimit, strconv.Itoa(opts.Limit))
	}
	if opts.Page > 0 {
		q.Set(common.QueryParamPage, strconv.Itoa(opts.Page))
	}
	if opts.URL != "" {
		q.Set(common.QueryParamURL, opts.URL)
	}
	var out ListRecordsResponse
	err := s.requestor.Do(ctx, http.MethodGet, common.IndexdIndexEndpoint, nil, &out, request.WithQueryValues(q))
	return out, err
}

func (s *IndexService) DeleteByQuery(ctx context.Context, opts DeleteByQueryOptions) (DeleteByQueryResponse, error) {
	q := url.Values{}
	if opts.Authz != "" {
		q.Set(common.QueryParamAuthz, opts.Authz)
	}
	if opts.Organization != "" {
		q.Set(common.QueryParamOrganization, opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set(common.QueryParamProject, opts.ProjectID)
	}
	if opts.Hash != "" {
		q.Set(common.QueryParamHash, opts.Hash)
	}
	if opts.HashType != "" {
		q.Set(common.QueryParamHashType, opts.HashType)
	}
	var out DeleteByQueryResponse
	err := s.requestor.Do(ctx, http.MethodDelete, common.IndexdIndexEndpoint, nil, &out, request.WithQueryValues(q))
	return out, err
}

func (s *IndexService) CreateBulk(ctx context.Context, req internalapi.BulkCreateRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkEndpoint, req, &out)
	return out, err
}

func (s *IndexService) BulkHashes(ctx context.Context, req BulkHashesRequest) (ListRecordsResponse, error) {
	var out ListRecordsResponse
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkHashesEndpoint, req, &out)
	return out, err
}

func (s *IndexService) DeleteBulk(ctx context.Context, req internalapi.BulkHashesRequest) (int, error) {
	var out struct {
		Deleted *int32 `json:"deleted"`
	}
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkDeleteEndpoint, req, &out)
	if err != nil {
		return 0, err
	}
	if out.Deleted == nil {
		return 0, nil
	}
	return int(*out.Deleted), nil
}

func (s *IndexService) BulkSHA256Validity(ctx context.Context, req BulkSHA256ValidityRequest) (map[string]bool, error) {
	out := map[string]bool{}
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkSHA256ValidityEndpoint, req, &out)
	return out, err
}

func (s *IndexService) BulkDocuments(ctx context.Context, dids []string) ([]InternalRecordResponse, error) {
	var out []InternalRecordResponse
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkDocumentsEndpoint, dids, &out)
	return out, err
}

func (s *IndexService) SHA256Validity(ctx context.Context, values []string) (map[string]bool, error) {
	payload := map[string][]string{"sha256": values}
	out := map[string]bool{}
	err := s.requestor.Do(ctx, http.MethodPost, common.IndexdIndexBulkSHA256ValidityEndpoint, payload, &out)
	return out, err
}

func (s *IndexService) Upsert(ctx context.Context, did, objectURL, fileName string, size int64, sha256sum string, authz []string) error {
	existing, err := s.Get(ctx, did)
	if err == nil {
		req := InternalRecordRequest{
			Did:          existing.Did,
			Authz:        existing.Authz,
			Description:  existing.Description,
			FileName:     existing.FileName,
			Hashes:       existing.Hashes,
			Size:         existing.Size,
			Urls:         existing.Urls,
			Version:      existing.Version,
			Organization: existing.Organization,
			Project:      existing.Project,
		}

		if strings.TrimSpace(req.Did) == "" {
			req.Did = did
		}
		if len(req.Authz) == 0 {
			if len(authz) == 0 {
				return fmt.Errorf("authz is required to upsert record %s", did)
			}
			req.Authz = append([]string(nil), authz...)
		}
		if fileName != "" {
			req.FileName = &fileName
		}
		if size > 0 {
			req.Size = &size
		}
		if objectURL != "" {
			var urls []string
			if req.Urls != nil {
				urls = *req.Urls
			}
			seen := map[string]bool{}
			for _, u := range urls {
				seen[u] = true
			}
			if !seen[objectURL] {
				urls = append(urls, objectURL)
				req.Urls = &urls
			}
		}
		if sha256sum != "" {
			if req.Hashes == nil {
				h := make(internalapi.HashInfo)
				req.Hashes = &h
			}
			(*req.Hashes)["sha256"] = sha256sum
		}
		_, err := s.Update(ctx, did, req)
		return err
	}

	payload := InternalRecordRequest{
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
