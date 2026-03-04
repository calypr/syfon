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

func errorResponseForDBError(ctx context.Context, err error) drs.ImplResponse {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		code := http.StatusForbidden
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			code = http.StatusUnauthorized
		}
		return drs.ImplResponse{Code: code, Body: drs.Error{Msg: "unauthorized", StatusCode: int32(code)}}
	case errors.Is(err, core.ErrNotFound):
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: "not found", StatusCode: http.StatusNotFound}}
	default:
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}
	}
}

func unauthorizedStatus(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func (s *ObjectsAPIService) GetObject(ctx context.Context, id string, expand bool) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, err)
		return resp, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: obj}, nil
}

func (s *ObjectsAPIService) PostObject(ctx context.Context, id string, req drs.PostObjectRequest) (drs.ImplResponse, error) {
	return s.GetObject(ctx, id, false)
}

func (s *ObjectsAPIService) OptionsObject(ctx context.Context, id string) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, err)
		return resp, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: authorizationsForObject(obj)}, nil
}

func (s *ObjectsAPIService) DeleteObject(ctx context.Context, id string, req drs.DeleteRequest) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, err)
		return resp, err
	}
	targetResources := obj.Authorizations
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !core.HasMethodAccess(ctx, "delete", targetResources) {
		return drs.ImplResponse{Code: http.StatusForbidden, Body: drs.Error{Msg: "forbidden: missing delete permission", StatusCode: http.StatusForbidden}}, nil
	}

	err = s.db.DeleteObject(ctx, id)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusNotFound}}, err
	}
	return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) BulkDeleteObjects(ctx context.Context, req drs.BulkDeleteRequest) (drs.ImplResponse, error) {
	if len(req.BulkObjectIds) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "bulk_object_ids cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	for _, id := range req.BulkObjectIds {
		obj, err := s.db.GetObject(ctx, id)
		if err != nil {
			resp := errorResponseForDBError(ctx, err)
			return resp, err
		}
		targetResources := obj.Authorizations
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(ctx, "delete", targetResources) {
			return drs.ImplResponse{Code: http.StatusForbidden, Body: drs.Error{Msg: "forbidden: missing delete permission", StatusCode: http.StatusForbidden}}, nil
		}
	}
	if err := s.db.BulkDeleteObjects(ctx, req.BulkObjectIds); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) GetBulkObjects(ctx context.Context, req drs.GetBulkObjectsRequest, expand bool) (drs.ImplResponse, error) {
	resolved := make([]drs.DrsObject, 0, len(req.BulkObjectIds))
	missing := make([]string, 0)
	denied := make([]string, 0)
	for _, id := range req.BulkObjectIds {
		obj, err := s.db.GetObject(ctx, id)
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) {
				denied = append(denied, id)
				continue
			}
			missing = append(missing, id)
			continue
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
			denied = append(denied, id)
			continue
		}
		resolved = append(resolved, *obj)
	}
	out := drs.GetBulkObjects200Response{
		Summary: drs.Summary{
			Requested:  int32(len(req.BulkObjectIds)),
			Resolved:   int32(len(resolved)),
			Unresolved: int32(len(missing)),
		},
		ResolvedDrsObject: resolved,
	}
	if len(denied) > 0 {
		out.UnresolvedDrsObjects = append(out.UnresolvedDrsObjects, drs.UnresolvedInner{
			ErrorCode: int32(unauthorizedStatus(ctx)),
			ObjectIds: uniqueStrings(denied),
		})
	}
	if len(missing) > 0 {
		out.UnresolvedDrsObjects = append(out.UnresolvedDrsObjects, drs.UnresolvedInner{
			ErrorCode: 404,
			ObjectIds: uniqueStrings(missing),
		})
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func (s *ObjectsAPIService) OptionsBulkObject(ctx context.Context, req drs.BulkObjectIdNoPassport) (drs.ImplResponse, error) {
	resolved := make([]drs.Authorizations, 0, len(req.BulkObjectIds))
	missing := make([]string, 0)
	denied := make([]string, 0)
	for _, id := range req.BulkObjectIds {
		obj, err := s.db.GetObject(ctx, id)
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) {
				denied = append(denied, id)
				continue
			}
			missing = append(missing, id)
			continue
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
			denied = append(denied, id)
			continue
		}
		auth := authorizationsForObject(obj)
		auth.DrsObjectId = id
		resolved = append(resolved, auth)
	}
	out := drs.OptionsBulkObject200Response{
		Summary: drs.Summary{
			Requested:  int32(len(req.BulkObjectIds)),
			Resolved:   int32(len(resolved)),
			Unresolved: int32(len(missing)),
		},
		ResolvedDrsObject: resolved,
	}
	if len(denied) > 0 {
		out.UnresolvedDrsObjects = append(out.UnresolvedDrsObjects, drs.UnresolvedInner{
			ErrorCode: int32(unauthorizedStatus(ctx)),
			ObjectIds: uniqueStrings(denied),
		})
	}
	if len(missing) > 0 {
		out.UnresolvedDrsObjects = append(out.UnresolvedDrsObjects, drs.UnresolvedInner{
			ErrorCode: 404,
			ObjectIds: uniqueStrings(missing),
		})
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
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
		accessMethods := make([]drs.AccessMethod, 0, len(c.AccessMethods))
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
			accessMethods = append(accessMethods, drs.AccessMethod{
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
		if len(authz) == 0 && (c.Organization != "" || c.Project != "") {
			if strings.TrimSpace(c.Project) != "" && strings.TrimSpace(c.Organization) == "" {
				return drs.ImplResponse{
					Code: http.StatusBadRequest,
					Body: drs.Error{
						Msg:        "candidate[" + strconv.Itoa(i) + "] project requires organization",
						StatusCode: http.StatusBadRequest,
					},
				}, nil
			}
			path := core.ResourcePathForScope(c.Organization, c.Project)
			if path != "" {
				authz = append(authz, path)
			}
		}
		obj.Authorizations = authz
		for i := range accessMethods {
			accessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: authz,
			}
		}
		obj.AccessMethods = accessMethods
		targetResources := authz
		// Indexd-compatible fallback for file upload flows when no explicit authz is provided.
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
			if !core.HasMethodAccess(ctx, "file_upload", targetResources) && !core.HasMethodAccess(ctx, "create", targetResources) {
				return drs.ImplResponse{
					Code: http.StatusForbidden,
					Body: drs.Error{
						Msg:        "forbidden: missing file_upload/create permission on /data_file",
						StatusCode: http.StatusForbidden,
					},
				}, nil
			}
		} else if !core.HasMethodAccess(ctx, "create", targetResources) {
			if !core.HasMethodAccess(ctx, "file_upload", []string{"/data_file"}) {
				return drs.ImplResponse{
					Code: http.StatusForbidden,
					Body: drs.Error{
						Msg:        "forbidden: missing create permission",
						StatusCode: http.StatusForbidden,
					},
				}, nil
			}
		}
		objects = append(objects, core.DrsObjectWithAuthz{DrsObject: obj, Authz: authz})
		registered = append(registered, obj)
	}

	if err := s.db.RegisterObjects(ctx, objects); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}

	return drs.ImplResponse{
		Code: http.StatusCreated,
		Body: drs.RegisterObjects201Response{
			Objects: registered,
		},
	}, nil
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
		resp := errorResponseForDBError(ctx, err)
		return resp, err
	}
	if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
		code := unauthorizedStatus(ctx)
		return drs.ImplResponse{Code: code, Body: drs.Error{Msg: "unauthorized", StatusCode: int32(code)}}, nil
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
	var results []drs.BulkAccessUrl
	unresolvedByCode := map[int32][]string{}
	requested := 0
	for _, mapping := range req.BulkObjectAccessIds {
		for _, accessID := range mapping.BulkAccessIds {
			requested++
			resp, err := s.GetAccessURL(ctx, mapping.BulkObjectId, accessID)
			if err != nil || resp.Code != http.StatusOK {
				code := int32(resp.Code)
				if code == 0 {
					code = http.StatusInternalServerError
				}
				unresolvedByCode[code] = append(unresolvedByCode[code], mapping.BulkObjectId)
				continue
			}
			accessURL, ok := resp.Body.(drs.AccessMethodAccessUrl)
			if !ok {
				unresolvedByCode[http.StatusInternalServerError] = append(unresolvedByCode[http.StatusInternalServerError], mapping.BulkObjectId)
				continue
			}
			results = append(results, drs.BulkAccessUrl{
				DrsObjectId: mapping.BulkObjectId,
				DrsAccessId: accessID,
				Url:         accessURL.Url,
				Headers:     accessURL.Headers,
			})
		}
	}
	out := drs.GetBulkAccessUrl200Response{
		Summary: drs.Summary{
			Requested:  int32(requested),
			Resolved:   int32(len(results)),
			Unresolved: 0,
		},
		ResolvedDrsObjectAccessUrls: results,
	}
	for code, ids := range unresolvedByCode {
		uniq := uniqueStrings(ids)
		out.Summary.Unresolved += int32(len(uniq))
		out.UnresolvedDrsObjects = append(out.UnresolvedDrsObjects, drs.UnresolvedInner{
			ErrorCode: code,
			ObjectIds: uniq,
		})
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func (s *ObjectsAPIService) UpdateObjectAccessMethods(ctx context.Context, objectID string, req drs.AccessMethodUpdateRequest) (drs.ImplResponse, error) {
	existing, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusNotFound}}, err
	}
	targetResources := existing.Authorizations
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !core.HasMethodAccess(ctx, "update", targetResources) {
		return drs.ImplResponse{Code: http.StatusForbidden, Body: drs.Error{Msg: "forbidden: missing update permission", StatusCode: http.StatusForbidden}}, nil
	}

	if err := s.db.UpdateObjectAccessMethods(ctx, objectID, req.AccessMethods); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	updated, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: updated}, nil
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
	updated := make([]drs.DrsObject, 0, len(req.Updates))
	for _, u := range req.Updates {
		obj, err := s.db.GetObject(ctx, u.ObjectId)
		if err != nil {
			resp := errorResponseForDBError(ctx, err)
			return resp, err
		}
		targetResources := obj.Authorizations
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(ctx, "update", targetResources) {
			code := unauthorizedStatus(ctx)
			return drs.ImplResponse{
				Code: code,
				Body: drs.Error{Msg: "forbidden: missing update permission", StatusCode: int32(code)},
			}, nil
		}
	}
	for _, update := range req.Updates {
		updates[update.ObjectId] = update.AccessMethods
	}
	if err := s.db.BulkUpdateAccessMethods(ctx, updates); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	for _, u := range req.Updates {
		obj, err := s.db.GetObject(ctx, u.ObjectId)
		if err != nil {
			continue
		}
		updated = append(updated, *obj)
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drs.BulkUpdateAccessMethods200Response{Objects: updated}}, nil
}

func (s *ObjectsAPIService) GetServiceInfo(ctx context.Context) (drs.ImplResponse, error) {
	info, err := s.db.GetServiceInfo(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: info}, nil
}

func authorizationsForObject(obj *drs.DrsObject) drs.Authorizations {
	authz := uniqueStrings(obj.Authorizations)
	if len(authz) == 0 {
		for _, am := range obj.AccessMethods {
			authz = append(authz, am.Authorizations.BearerAuthIssuers...)
		}
		authz = uniqueStrings(authz)
	}
	return drs.Authorizations{
		BearerAuthIssuers: authz,
		SupportedTypes:    []string{"BearerAuth"},
	}
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
