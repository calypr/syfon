package syfonclient

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/transfer"
)

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
	listResp, err := s.index.List(ctx, ListRecordsOptions{
		Hash: strings.Join(hashes, ","),
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
	authz := rec.Authz
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
		if len(authz) > 0 {
			method.Authorizations = &struct {
				BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
				DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
				PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
				SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
			}{
				BearerAuthIssuers: &authz,
			}
		}
		ams = append(ams, method)
	}
	if len(ams) > 0 {
		obj.AccessMethods = &ams
	}
	return obj
}
