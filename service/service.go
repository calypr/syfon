package service

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

// ObjectsAPIService implements the Objects API service.
type ObjectsAPIService struct {
	db         core.DatabaseInterface
	urlManager urlmanager.UrlManager
}

// NewObjectsAPIService creates a new ObjectsAPIService.
func NewObjectsAPIService(db core.DatabaseInterface, urlManager urlmanager.UrlManager) *ObjectsAPIService {
	return &ObjectsAPIService{db: db, urlManager: urlManager}
}

func (s *ObjectsAPIService) GetObject(ctx context.Context, id string, expand bool) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusNotFound}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: obj}, nil
}

func (s *ObjectsAPIService) PostObject(ctx context.Context, id string, req drs.PostObjectRequest) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) OptionsObject(ctx context.Context, id string) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusOK, Body: nil}, nil
}

func (s *ObjectsAPIService) DeleteObject(ctx context.Context, id string, req drs.DeleteRequest) (drs.ImplResponse, error) {
	err := s.db.DeleteObject(ctx, id)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusNotFound}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: id}, nil
}

func (s *ObjectsAPIService) BulkDeleteObjects(ctx context.Context, req drs.BulkDeleteRequest) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) GetBulkObjects(ctx context.Context, req drs.GetBulkObjectsRequest, expand bool) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) OptionsBulkObject(ctx context.Context, req drs.BulkObjectIdNoPassport) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusOK, Body: nil}, nil
}

func (s *ObjectsAPIService) RegisterObjects(ctx context.Context, req drs.RegisterObjectsRequest) (drs.ImplResponse, error) {
	var objects []core.DrsObjectWithAuthz
	now := time.Now()
	registered := make([]drs.DrsObject, 0, len(req.Candidates))

	for i, c := range req.Candidates {
		primaryChecksum, ok := canonicalSHA256(c.Checksums)
		if !ok {
			return drs.ImplResponse{
				Code: http.StatusBadRequest,
				Body: drs.Error{
					Msg:        "candidate[" + strconv.Itoa(i) + "] must include a sha256 checksum",
					StatusCode: http.StatusBadRequest,
				},
			}, nil
		}
		id := primaryChecksum

		obj := drs.DrsObject{
			Id:          id,
			Name:        c.Name,
			Size:        c.Size,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
			Description: c.Description,
			Aliases:     c.Aliases,
			SelfUri:     "drs://" + id,
		}
		obj.Checksums = []drs.Checksum{{Type: "sha256", Checksum: primaryChecksum}}

		seenAccess := make(map[string]struct{})
		seenAuthz := make(map[string]struct{})
		authz := make([]string, 0)
		for _, am := range c.AccessMethods {
			if am.AccessUrl.Url == "" {
				continue
			}
			accessKey := am.Type + "|" + am.AccessUrl.Url
			if _, exists := seenAccess[accessKey]; exists {
				continue
			}
			seenAccess[accessKey] = struct{}{}

			accessID := am.AccessId
			if accessID == "" {
				accessID = am.Type
			}
			obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
				Type:      am.Type,
				AccessUrl: am.AccessUrl,
				AccessId:  accessID,
				Region:    am.Region,
			})
			for _, issuer := range am.Authorizations.BearerAuthIssuers {
				if issuer == "" {
					continue
				}
				if _, ok := seenAuthz[issuer]; ok {
					continue
				}
				seenAuthz[issuer] = struct{}{}
				authz = append(authz, issuer)
			}
		}
		obj.Authorizations = authz
		objects = append(objects, core.DrsObjectWithAuthz{DrsObject: obj, Authz: authz})
		registered = append(registered, obj)
	}

	if err := s.db.RegisterObjects(ctx, objects); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}

	return drs.ImplResponse{Code: http.StatusOK, Body: registered}, nil
}

func (s *ObjectsAPIService) GetObjectsByChecksums(ctx context.Context, checksums []string) (drs.ImplResponse, error) {
	objsMap, err := s.db.GetObjectsByChecksums(ctx, checksums)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: objsMap}, nil
}

func (s *ObjectsAPIService) GetAccessURL(ctx context.Context, objectID string, accessID string) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusNotFound}}, err
	}

	var selectedAccessMethod *drs.AccessMethod
	for _, method := range obj.AccessMethods {
		if method.AccessId == accessID {
			selectedAccessMethod = &method
			break
		}
	}

	if selectedAccessMethod == nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: "access_id not found", StatusCode: http.StatusNotFound}}, nil
	}

	if selectedAccessMethod.AccessUrl.Url == "" {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: "access method has no URL", StatusCode: http.StatusInternalServerError}}, nil
	}

	signedURL, err := s.urlManager.SignURL(ctx, accessID, selectedAccessMethod.AccessUrl.Url, urlmanager.SignOptions{})
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}

	return drs.ImplResponse{
		Code: http.StatusOK,
		Body: drs.AccessMethodAccessUrl{
			Url: signedURL,
		},
	}, nil
}

func (s *ObjectsAPIService) PostAccessURL(ctx context.Context, objectID string, accessID string, req drs.PostAccessUrlRequest) (drs.ImplResponse, error) {
	return s.GetAccessURL(ctx, objectID, accessID)
}

func (s *ObjectsAPIService) GetBulkAccessURL(ctx context.Context, req drs.BulkObjectAccessId) (drs.ImplResponse, error) {
	var results []drs.AccessMethodAccessUrl
	for _, mapping := range req.BulkObjectAccessIds {
		for _, accessID := range mapping.BulkAccessIds {
			resp, err := s.GetAccessURL(ctx, mapping.BulkObjectId, accessID)
			if err != nil {
				return resp, err
			}
			results = append(results, resp.Body.(drs.AccessMethodAccessUrl))
		}
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: results}, nil
}

func (s *ObjectsAPIService) UpdateObjectAccessMethods(ctx context.Context, objectID string, req drs.AccessMethodUpdateRequest) (drs.ImplResponse, error) {
	if err := s.db.UpdateObjectAccessMethods(ctx, objectID, req.AccessMethods); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: nil}, nil
}

func (s *ObjectsAPIService) GetObjectsByChecksum(ctx context.Context, checksum string) (drs.ImplResponse, error) {
	objs, err := s.db.GetObjectsByChecksum(ctx, normalizeChecksum(checksum))
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	if len(objs) == 0 {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: "object not found for checksum", StatusCode: http.StatusNotFound}}, nil
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: objs[0]}, nil
}

func normalizeChecksum(cs string) string {
	if parts := strings.SplitN(cs, ":", 2); len(parts) == 2 {
		return parts[1]
	}
	return cs
}

func canonicalSHA256(checksums []drs.Checksum) (string, bool) {
	for _, cs := range checksums {
		checksumType := strings.ToLower(strings.TrimSpace(cs.Type))
		if checksumType == "sha256" || checksumType == "sha-256" {
			normalized := normalizeChecksum(strings.TrimSpace(cs.Checksum))
			if normalized != "" {
				return normalized, true
			}
		}
	}
	return "", false
}

func (s *ObjectsAPIService) BulkUpdateAccessMethods(ctx context.Context, req drs.BulkAccessMethodUpdateRequest) (drs.ImplResponse, error) {
	updates := make(map[string][]drs.AccessMethod)
	for _, update := range req.Updates {
		updates[update.ObjectId] = update.AccessMethods
	}
	if err := s.db.BulkUpdateAccessMethods(ctx, updates); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: nil}, nil
}

func (s *ObjectsAPIService) GetServiceInfo(ctx context.Context) (drs.ImplResponse, error) {
	info, err := s.db.GetServiceInfo(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: info}, nil
}
