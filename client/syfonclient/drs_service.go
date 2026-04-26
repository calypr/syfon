package syfonclient

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/transfer"
)

var ErrNoRecordsForHash = errors.New("no records found for hash")

type DRSService struct {
	gen   drsapi.ClientWithResponsesInterface
	index *IndexService
}

func NewDRSService(gen drsapi.ClientWithResponsesInterface, index *IndexService) *DRSService {
	return &DRSService{gen: gen, index: index}
}

// Resolve implements transfer.Resolver by fetching object metadata from the DRS API.
func (s *DRSService) Resolve(ctx context.Context, id string) (*transfer.ResolvedObject, error) {
	obj, err := s.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) == 0 {
		return nil, fmt.Errorf("no access methods found for object %s", id)
	}
	am := (*obj.AccessMethods)[0]
	name := ""
	if obj.Name != nil {
		name = *obj.Name
	}
	url := ""
	if am.AccessUrl != nil {
		url = am.AccessUrl.Url
	}
	return &transfer.ResolvedObject{
		Id:           obj.Id,
		Name:         name,
		Size:         obj.Size,
		ProviderURL:  url,
		AccessMethod: string(am.Type),
	}, nil
}

func (s *DRSService) GetObject(ctx context.Context, objectID string) (drsapi.DrsObject, error) {
	resp, err := s.gen.GetObjectWithResponse(ctx, drsapi.ObjectId(objectID), nil)
	if err != nil {
		return drsapi.DrsObject{}, err
	}
	if resp.JSON200 == nil {
		return drsapi.DrsObject{}, fmt.Errorf("unexpected response: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *DRSService) ListObjects(ctx context.Context, limit, page int) (DRSPage, error) {
	listResp, err := s.index.List(ctx, ListRecordsOptions{
		Limit: limit,
		Page:  page,
	})
	if err != nil {
		return DRSPage{}, err
	}
	records := make([]internalapi.InternalRecord, 0)
	if listResp.Records != nil {
		records = *listResp.Records
	}
	out := DRSPage{
		DrsObjects: make([]drsapi.DrsObject, 0, len(records)),
	}
	for _, rec := range records {
		out.DrsObjects = append(out.DrsObjects, internalRecordToDRSObject(&rec))
	}
	return out, nil
}

func (s *DRSService) GetAccessURL(ctx context.Context, objectID, accessID string) (drsapi.AccessURL, error) {
	resp, err := s.gen.GetAccessURLWithResponse(ctx, drsapi.ObjectId(objectID), drsapi.AccessId(accessID))
	if err != nil {
		return drsapi.AccessURL{}, err
	}
	if resp.JSON200 == nil {
		return drsapi.AccessURL{}, fmt.Errorf("unexpected response: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *DRSService) RegisterObjects(ctx context.Context, req drsapi.RegisterObjectsJSONRequestBody) (drsapi.N201ObjectsCreated, error) {
	resp, err := s.gen.RegisterObjectsWithResponse(ctx, drsapi.RegisterObjectsJSONRequestBody(req))
	if err != nil {
		return drsapi.N201ObjectsCreated{}, err
	}
	if resp.JSON201 == nil {
		return drsapi.N201ObjectsCreated{}, fmt.Errorf("unexpected response: %d", resp.StatusCode())
	}
	return *resp.JSON201, nil
}

func (s *DRSService) ListObjectsByProject(ctx context.Context, projectID string, limit, page int) (DRSPage, error) {
	listResp, err := s.index.List(ctx, ListRecordsOptions{
		ProjectID: projectID,
		Limit:     limit,
		Page:      page,
	})
	if err != nil {
		return DRSPage{}, err
	}
	records := make([]internalapi.InternalRecord, 0)
	if listResp.Records != nil {
		records = *listResp.Records
	}
	out := DRSPage{
		DrsObjects: make([]drsapi.DrsObject, 0, len(records)),
	}
	for _, rec := range records {
		out.DrsObjects = append(out.DrsObjects, internalRecordToDRSObject(&rec))
	}
	return out, nil
}

func (s *DRSService) GetProjectSample(ctx context.Context, projectID string, limit int) (DRSPage, error) {
	return s.ListObjectsByProject(ctx, projectID, limit, 1)
}

func (s *DRSService) BatchGetObjectsByHash(ctx context.Context, hashes []string) (DRSPage, error) {
	out := DRSPage{DrsObjects: make([]drsapi.DrsObject, 0, len(hashes))}
	seen := make(map[string]struct{})
	for _, h := range hashes {
		checksum := normalizeChecksum(h)
		if checksum == "" {
			continue
		}
		resp, err := s.gen.GetObjectsByChecksumWithResponse(ctx, drsapi.ChecksumParam(checksum))
		if err != nil {
			continue
		}
		if resp.JSON200 == nil || resp.JSON200.ResolvedDrsObject == nil {
			continue
		}
		for _, obj := range *resp.JSON200.ResolvedDrsObject {
			id := strings.TrimSpace(obj.Id)
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			out.DrsObjects = append(out.DrsObjects, obj)
		}
	}
	return out, nil
}

func (s *DRSService) GetObjectsByHashForResource(ctx context.Context, hash string, organization string, project string) ([]drsapi.DrsObject, error) {
	organization = strings.TrimSpace(organization)
	if organization == "" {
		return nil, nil
	}
	page, err := s.BatchGetObjectsByHash(ctx, []string{hash})
	if err != nil {
		return nil, err
	}
	project = strings.TrimSpace(project)
	result := make([]drsapi.DrsObject, 0, len(page.DrsObjects))
	for _, obj := range page.DrsObjects {
		if drsObjectMatchesAuthzScope(&obj, organization, project) {
			result = append(result, obj)
		}
	}
	return result, nil
}

func (s *DRSService) ResolveResourceAccessURL(ctx context.Context, hash string, organization string, project string) (*drsapi.AccessURL, error) {
	records, err := s.GetObjectsByHashForResource(ctx, hash, organization, project)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%w %s", ErrNoRecordsForHash, hash)
	}

	matchingRecord := records[0]
	if matchingRecord.AccessMethods == nil || len(*matchingRecord.AccessMethods) == 0 {
		return nil, fmt.Errorf("no access methods available for DRS object %s", matchingRecord.Id)
	}

	accessType := (*matchingRecord.AccessMethods)[0].Type
	if accessType == "" {
		return nil, fmt.Errorf("no access type found in access method for DRS object %s", matchingRecord.Id)
	}

	accessURL, err := s.GetAccessURL(ctx, matchingRecord.Id, string(accessType))
	if err != nil {
		return nil, err
	}
	return &accessURL, nil
}

func (s *DRSService) DeleteRecordsByHash(ctx context.Context, hash string) error {
	hash = normalizeChecksum(hash)
	page, err := s.BatchGetObjectsByHash(ctx, []string{hash})
	if err != nil {
		return fmt.Errorf("error resolving DRS object for hash %s: %w", hash, err)
	}
	if len(page.DrsObjects) == 0 {
		return fmt.Errorf("%w %s", ErrNoRecordsForHash, hash)
	}

	seen := make(map[string]struct{}, len(page.DrsObjects))
	for _, record := range page.DrsObjects {
		did := strings.TrimSpace(record.Id)
		if did == "" {
			continue
		}
		if _, exists := seen[did]; exists {
			continue
		}
		seen[did] = struct{}{}
		if err := s.index.Delete(ctx, did); err != nil {
			return fmt.Errorf("error deleting DID %s for hash %s: %w", did, hash, err)
		}
	}
	if len(seen) == 0 {
		return fmt.Errorf("no deleteable DIDs found for hash %s", hash)
	}
	return nil
}

func normalizeChecksum(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "sha256:")
	return strings.TrimSpace(raw)
}

func drsObjectMatchesAuthzScope(obj *drsapi.DrsObject, organization, project string) bool {
	if obj == nil || obj.AccessMethods == nil {
		return false
	}
	for _, method := range *obj.AccessMethods {
		if method.Authorizations == nil {
			continue
		}
		if authzMapMatchesScope(*method.Authorizations, organization, project) {
			return true
		}
	}
	return false
}

func authzMapMatchesScope(authorizations map[string][]string, organization, project string) bool {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || len(authorizations) == 0 {
		return false
	}
	projects, ok := authorizations[organization]
	if !ok {
		return false
	}
	if len(projects) == 0 {
		return true
	}
	for _, candidate := range projects {
		if strings.TrimSpace(candidate) == project {
			return true
		}
	}
	return false
}

func internalRecordToDRSObject(rec *internalapi.InternalRecord) drsapi.DrsObject {
	size := int64(0)
	if rec.Size != nil {
		size = *rec.Size
	}
	obj := drsapi.DrsObject{
		Id:      rec.Did,
		SelfUri: "drs://" + rec.Did,
		Size:    size,
		Name:    rec.FileName,
	}
	if rec.Hashes != nil {
		hInfo := *rec.Hashes
		obj.Checksums = make([]drsapi.Checksum, 0, len(hInfo))
		for typ, checksum := range hInfo {
			obj.Checksums = append(obj.Checksums, drsapi.Checksum{
				Type:     typ,
				Checksum: checksum,
			})
		}
	}

	var urls []string
	if rec.Urls != nil {
		urls = *rec.Urls
	}
	ams := make([]drsapi.AccessMethod, 0, len(urls))
	for _, rawURL := range urls {
		method := drsapi.AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: rawURL},
			Type: "https",
		}
		if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" {
			method.Type = drsapi.AccessMethodType(parsed.Scheme)
		}
		if rec.Authorizations != nil {
			method.Authorizations = rec.Authorizations
		}
		ams = append(ams, method)
	}
	if len(ams) > 0 {
		obj.AccessMethods = &ams
	}
	return obj
}
