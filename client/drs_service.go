package client

import (
	"context"
	"net/url"
)

type DRSService struct {
	c *Client
}

func (s *DRSService) GetObject(ctx context.Context, objectID string) (DRSObject, error) {
	var out DRSObject
	err := s.c.doJSON(ctx, "GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID), nil, nil, &out)
	return out, err
}

func (s *DRSService) ListObjects(ctx context.Context, limit, page int) (DRSPage, error) {
	listResp, err := s.c.Index().List(ctx, ListRecordsOptions{
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
	err := s.c.doJSON(ctx, "GET", "/ga4gh/drs/v1/objects/"+url.PathEscape(objectID)+"/access/"+url.PathEscape(accessID), nil, nil, &out)
	return out, err
}

func (s *DRSService) RegisterObjects(ctx context.Context, req RegisterObjectsRequest) (RegisterObjectsResponse, error) {
	var out RegisterObjectsResponse
	err := s.c.doJSON(ctx, "POST", "/ga4gh/drs/v1/objects/register", nil, req, &out)
	return out, err
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
