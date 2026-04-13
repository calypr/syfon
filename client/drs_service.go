package client

import (
	"context"
	"fmt"
	"net/url"

	"github.com/calypr/syfon/client/xfer"
)

type DRSService struct {
	base  *baseService
	index *IndexService
}

// Resolve implements xfer.Resolver by fetching object metadata from the DRS API.
func (s *DRSService) Resolve(ctx context.Context, id string) (*xfer.ResolvedObject, error) {
	obj, err := s.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	// Convert DRSObject to transfer.ResolvedObject.
	// For simplicity, we assume the first access method is the primary one.
	if len(obj.AccessMethods) == 0 {
		return nil, fmt.Errorf("no access methods found for object %s", id)
	}
	am := obj.AccessMethods[0]
	return &xfer.ResolvedObject{
		Id:           obj.Id,
		Name:         obj.Name,
		Size:         obj.Size,
		ProviderURL:  am.AccessUrl.Url,
		AccessMethod: am.Type,
	}, nil
}

func (s *DRSService) GetObject(ctx context.Context, objectID string) (DRSObject, error) {
	var out DRSObject
	rb := s.base.requestor.New("GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID))
	err := s.base.requestor.DoJSON(ctx, rb, &out)
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
	out := DRSPage{
		DrsObjects: make([]DRSObject, 0, len(listResp.GetRecords())),
	}
	for _, rec := range listResp.GetRecords() {
		out.DrsObjects = append(out.DrsObjects, internalRecordToDRSObject(rec))
	}
	return out, nil
}

func (s *DRSService) GetAccessURL(ctx context.Context, objectID, accessID string) (AccessMethodAccessURL, error) {
	var out AccessMethodAccessURL
	rb := s.base.requestor.New("GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID)+"/access/"+url.PathEscape(accessID))
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *DRSService) RegisterObjects(ctx context.Context, req RegisterObjectsRequest) (RegisterObjectsResponse, error) {
	var out RegisterObjectsResponse
	rb, err := s.base.requestor.New("POST", "/ga4gh/drs/v1/objects/register").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	doneErr := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, doneErr
}

func internalRecordToDRSObject(rec InternalRecord) DRSObject {
	obj := DRSObject{
		Id:        rec.GetDid(),
		SelfUri:   "drs://" + rec.GetDid(),
		Size:      rec.GetSize(),
		Name:      rec.GetFileName(),
		Checksums: make([]Checksum, 0, len(rec.GetHashes())),
	}
	for typ, checksum := range rec.GetHashes() {
		obj.Checksums = append(obj.Checksums, Checksum{
			Type:     typ,
			Checksum: checksum,
		})
	}

	urls := rec.GetUrls()
	authz := rec.GetAuthz()
	obj.AccessMethods = make([]AccessMethod, 0, len(urls))
	for _, rawURL := range urls {
		method := AccessMethod{
			AccessUrl: AccessMethodAccessURL{Url: rawURL},
			Type:      "https",
		}
		if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" {
			method.Type = parsed.Scheme
		}
		if len(authz) > 0 {
			method.Authorizations = AccessMethodAuthorizations{
				BearerAuthIssuers: append([]string(nil), authz...),
			}
		}
		obj.AccessMethods = append(obj.AccessMethods, method)
	}
	return obj
}
