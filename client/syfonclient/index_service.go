package syfonclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
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
	params := url.Values{}
	if opts.Hash != "" {
		params.Set("hash", opts.Hash)
	}
	if opts.Organization != "" {
		params.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		params.Set("project", opts.ProjectID)
	}
	if opts.Limit != 0 {
		params.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}
	if opts.Page != 0 {
		params.Set("page", fmt.Sprintf("%d", opts.Page))
	}
	var out internalapi.ListRecordsResponse
	if err := s.requestor.Do(ctx, http.MethodGet, "/index", nil, &out, request.WithQueryValues(params)); err != nil {
		return internalapi.ListRecordsResponse{}, err
	}
	return out, nil
}

func (s *IndexService) DeleteByQuery(ctx context.Context, opts DeleteByQueryOptions) (internalapi.DeleteByQueryResponse, error) {
	params := &internalapi.InternalDeleteByQueryParams{}
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

func (s *IndexService) Upsert(ctx context.Context, did, objectURL, fileName string, size int64, sha256sum string, authorizations map[string][]string) error {
	existing, err := s.Get(ctx, did)
	if err == nil {
		req := internalapi.InternalRecord{
			Did:          existing.Did,
			Auth:         existing.Auth,
			Description:  existing.Description,
			FileName:     existing.FileName,
			Hashes:       existing.Hashes,
			Size:         existing.Size,
			Version:      existing.Version,
			Organization: existing.Organization,
			Project:      existing.Project,
		}

		if strings.TrimSpace(req.Did) == "" {
			req.Did = did
		}
		if req.Auth == nil || len(*req.Auth) == 0 {
			if len(authorizations) == 0 {
				return fmt.Errorf("authorizations are required to upsert record %s", did)
			}
			auth := authPathMapForURL(objectURL, authorizations)
			req.Auth = &auth
		}
		if fileName != "" {
			req.FileName = &fileName
		}
		if size > 0 {
			req.Size = &size
		}
		if objectURL != "" {
			if req.Auth == nil {
				auth := make(internalapi.AuthPathMap)
				req.Auth = &auth
			}
			appendURLToAuthPathMap(*req.Auth, objectURL, authorizations)

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
	if len(authorizations) == 0 {
		return fmt.Errorf("authorizations are required to create record %s", did)
	}
	auth := authPathMapForURL(objectURL, authorizations)
	payload.Auth = &auth
	if size > 0 {
		payload.Size = &size
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

func authPathMapForURL(rawURL string, authorizations map[string][]string) internalapi.AuthPathMap {
	auth := make(internalapi.AuthPathMap)
	appendURLToAuthPathMap(auth, rawURL, authorizations)
	return auth
}

func appendURLToAuthPathMap(auth internalapi.AuthPathMap, rawURL string, authorizations map[string][]string) {
	if rawURL == "" {
		return
	}
	for org, projects := range authorizations {
		if auth[org] == nil {
			auth[org] = make(map[string][]string)
		}
		if len(projects) == 0 {
			auth[org][""] = appendUniqueString(auth[org][""], rawURL)
			continue
		}
		for _, project := range projects {
			auth[org][project] = appendUniqueString(auth[org][project], rawURL)
		}
	}
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// --- IndexService ---
