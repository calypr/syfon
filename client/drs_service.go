package client

import (
	"context"
	"fmt"
	"net/url"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/xfer"
)

type DRSService struct {
	requestor request.Requester
	index     *IndexService
}

func NewDRSService(r request.Requester, index *IndexService) *DRSService {
	return &DRSService{requestor: r, index: index}
}

// Resolve implements xfer.Resolver by fetching object metadata from the DRS API.
func (s *DRSService) Resolve(ctx context.Context, id string) (*xfer.ResolvedObject, error) {
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
	return &xfer.ResolvedObject{
		Id:           obj.Id,
		Name:         name,
		Size:         obj.Size,
		ProviderURL:  url,
		AccessMethod: string(am.Type),
	}, nil
}

func (s *DRSService) GetObject(ctx context.Context, objectID string) (DRSObject, error) {
	var out DRSObject
	err := s.requestor.Do(ctx, "GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID), nil, &out)
	return out, err
}

func (s *DRSService) ListObjects(ctx context.Context, limit, page int) (DRSPage, error) {
	listResp, err := s.index.List(ctx, ListRecordsOptions{
		Limit: limit,
		Page:  page,
	})
	if err != nil {
		return DRSPage{}, err
	}
	records := make([]InternalRecordRequest, 0)
	if listResp.Records != nil {
		records = *listResp.Records
	}
	out := DRSPage{
		DrsObjects: make([]DRSObject, 0, len(records)),
	}
	for _, rec := range records {
		out.DrsObjects = append(out.DrsObjects, internalRecordToDRSObject(&rec))
	}
	return out, nil
}

func (s *DRSService) GetAccessURL(ctx context.Context, objectID, accessID string) (AccessMethodAccessURL, error) {
	var out AccessMethodAccessURL
	err := s.requestor.Do(ctx, "GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID)+"/access/"+url.PathEscape(accessID), nil, &out)
	return out, err
}

func (s *DRSService) RegisterObjects(ctx context.Context, req RegisterObjectsRequest) (RegisterObjectsResponse, error) {
	var out RegisterObjectsResponse
	err := s.requestor.Do(ctx, "POST", "/ga4gh/drs/v1/objects/register", req, &out)
	return out, err
}

func internalRecordToDRSObject(rec *InternalRecordRequest) DRSObject {
	size := int64(0)
	if rec.Size != nil {
		size = *rec.Size
	}
	obj := DRSObject{
		Id:      rec.Did,
		SelfUri: "drs://" + rec.Did,
		Size:    size,
		Name:    rec.FileName,
	}
	if rec.Hashes != nil {
		hInfo := *rec.Hashes
		obj.Checksums = make([]Checksum, 0, len(hInfo))
		for typ, checksum := range hInfo {
			obj.Checksums = append(obj.Checksums, Checksum{
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
	ams := make([]AccessMethod, 0, len(urls))
	for _, rawURL := range urls {
		method := AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: rawURL},
			Type: "https",
		}
		if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" {
			method.Type = drs.AccessMethodType(parsed.Scheme)
		}
		if len(authz) > 0 {
			method.Authorizations = &struct {
				BearerAuthIssuers   *[]string                                   `json:"bearer_auth_issuers,omitempty"`
				DrsObjectId         *string                                     `json:"drs_object_id,omitempty"`
				PassportAuthIssuers *[]string                                   `json:"passport_auth_issuers,omitempty"`
				SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
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
