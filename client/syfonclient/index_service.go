package syfonclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/request"
)

type IndexService struct {
	gen       internalapi.ClientWithResponsesInterface
	requestor request.Requester
}

func NewIndexService(gen internalapi.ClientWithResponsesInterface, r request.Requester) *IndexService {
	return &IndexService{gen: gen, requestor: r}
}

func (s *IndexService) Get(ctx context.Context, did string) (internalapi.InternalRecordResponse, error) {
	resp, err := s.gen.InternalGetWithResponse(ctx, did)
	if err != nil {
		return internalapi.InternalRecordResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalRecordResponse{}, fmt.Errorf("unexpected response: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) GetByHash(ctx context.Context, hash string) (internalapi.ListRecordsResponse, error) {
	params := &internalapi.InternalListParams{
		Hash: &hash,
	}
	resp, err := s.gen.InternalListWithResponse(ctx, params)
	if err != nil {
		return internalapi.ListRecordsResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.ListRecordsResponse{}, fmt.Errorf("failed to get record by hash: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) Create(ctx context.Context, rec internalapi.InternalRecord) (internalapi.InternalRecordResponse, error) {
	resp, err := s.gen.InternalCreateWithResponse(ctx, internalapi.InternalCreateJSONRequestBody(rec))
	if err != nil {
		return internalapi.InternalRecordResponse{}, err
	}
	if resp.JSON201 == nil {
		return internalapi.InternalRecordResponse{}, fmt.Errorf("failed to create record: %d", resp.StatusCode())
	}
	return *resp.JSON201, nil
}

func (s *IndexService) Update(ctx context.Context, did string, rec internalapi.InternalRecord) (internalapi.InternalRecordResponse, error) {
	resp, err := s.gen.InternalUpdateWithResponse(ctx, did, internalapi.InternalUpdateJSONRequestBody(rec))
	if err != nil {
		return internalapi.InternalRecordResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalRecordResponse{}, fmt.Errorf("failed to update record: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) Delete(ctx context.Context, did string) error {
	resp, err := s.gen.InternalDeleteWithResponse(ctx, did)
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("failed to delete record: %d", resp.StatusCode())
	}
	return nil
}

func (s *IndexService) List(ctx context.Context, opts ListRecordsOptions) (internalapi.ListRecordsResponse, error) {
	params := &internalapi.InternalListParams{}
	if opts.Hash != "" {
		params.Hash = &opts.Hash
	}
	if opts.Authz != "" {
		params.Authz = &opts.Authz
	}
	if opts.Organization != "" {
		params.Organization = &opts.Organization
	}
	if opts.ProjectID != "" {
		params.Project = &opts.ProjectID
	}
	if opts.Limit != 0 {
		params.Limit = &opts.Limit
	}
	if opts.Page != 0 {
		params.Page = &opts.Page
	}
	resp, err := s.gen.InternalListWithResponse(ctx, params)
	if err != nil {
		return internalapi.ListRecordsResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.ListRecordsResponse{}, fmt.Errorf("failed to list records: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) DeleteByQuery(ctx context.Context, opts DeleteByQueryOptions) (internalapi.DeleteByQueryResponse, error) {
	params := &internalapi.InternalDeleteByQueryParams{}
	if opts.Authz != "" {
		params.Authz = &opts.Authz
	}
	if opts.Organization != "" {
		params.Organization = &opts.Organization
	}
	if opts.ProjectID != "" {
		params.Project = &opts.ProjectID
	}
	if opts.Hash != "" {
		params.Hash = &opts.Hash
	}
	if opts.HashType != "" {
		params.HashType = &opts.HashType
	}

	resp, err := s.gen.InternalDeleteByQueryWithResponse(ctx, params)
	if err != nil {
		return internalapi.DeleteByQueryResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.DeleteByQueryResponse{}, fmt.Errorf("failed to delete by query: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) CreateBulk(ctx context.Context, req internalapi.BulkCreateRequest) (internalapi.ListRecordsResponse, error) {
	resp, err := s.gen.InternalBulkCreateWithResponse(ctx, internalapi.InternalBulkCreateJSONRequestBody(req))
	if err != nil {
		return internalapi.ListRecordsResponse{}, err
	}
	if resp.JSON201 == nil {
		return internalapi.ListRecordsResponse{}, fmt.Errorf("failed to bulk create: %d", resp.StatusCode())
	}
	return *resp.JSON201, nil
}

func (s *IndexService) BulkHashes(ctx context.Context, req internalapi.BulkHashesRequest) (internalapi.ListRecordsResponse, error) {
	resp, err := s.gen.InternalBulkHashesWithResponse(ctx, internalapi.InternalBulkHashesJSONRequestBody(req))
	if err != nil {
		return internalapi.ListRecordsResponse{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.ListRecordsResponse{}, fmt.Errorf("failed to bulk hashes: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) DeleteBulk(ctx context.Context, req internalapi.BulkHashesRequest) (int, error) {
	resp, err := s.gen.InternalBulkDeleteHashesWithResponse(ctx, internalapi.InternalBulkDeleteHashesJSONRequestBody(req))
	if err != nil {
		return 0, err
	}
	if resp.JSON200 == nil {
		return 0, fmt.Errorf("failed to bulk delete: %d", resp.StatusCode())
	}
	if resp.JSON200.Deleted == nil {
		return 0, nil
	}
	return int(*resp.JSON200.Deleted), nil
}

func (s *IndexService) BulkSHA256Validity(ctx context.Context, req internalapi.BulkSHA256ValidityRequest) (map[string]bool, error) {
	resp, err := s.gen.InternalBulkSHA256ValidityWithResponse(ctx, internalapi.InternalBulkSHA256ValidityJSONRequestBody(req))
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("failed to bulk sha256 validity: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) BulkDocuments(ctx context.Context, dids []string) ([]internalapi.InternalRecordResponse, error) {
	var body internalapi.BulkDocumentsRequest
	if err := body.FromBulkDocumentsRequest0(internalapi.BulkDocumentsRequest0(dids)); err != nil {
		return nil, err
	}
	resp, err := s.gen.InternalBulkDocumentsWithResponse(ctx, internalapi.InternalBulkDocumentsJSONRequestBody(body))
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("failed to bulk documents: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *IndexService) SHA256Validity(ctx context.Context, values []string) (map[string]bool, error) {
	return s.BulkSHA256Validity(ctx, internalapi.BulkSHA256ValidityRequest{Sha256: &values})
}

func (s *IndexService) Upsert(ctx context.Context, did, objectURL, fileName string, size int64, sha256sum string, authz []string) error {
	existing, err := s.Get(ctx, did)
	if err == nil {
		req := internalapi.InternalRecord{
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

	payload := internalapi.InternalRecord{
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
