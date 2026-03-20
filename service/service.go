package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/drs-server/adapter/drsmap"
	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

// ObjectsAPIService implements the Objects API service.
type ObjectsAPIService struct {
	db         core.DatabaseInterface
	urlManager urlmanager.UrlManager
}

const (
	defaultMaxBulkRequestLength            = 200
	defaultMaxBulkDeleteLength             = 100
	defaultMaxRegisterRequestLength        = 200
	defaultMaxBulkAccessMethodUpdateLength = 200
	defaultMaxBulkChecksumAdditionLength   = 200
	defaultMaxChecksumAdditionsPerObject   = 200
)

// NewObjectsAPIService creates a new ObjectsAPIService.
func NewObjectsAPIService(db core.DatabaseInterface, urlManager urlmanager.UrlManager) *ObjectsAPIService {
	return &ObjectsAPIService{db: db, urlManager: urlManager}
}

func errorResponseForDBError(ctx context.Context, op string, err error) drs.ImplResponse {
	requestID := core.GetRequestID(ctx)
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		code := http.StatusForbidden
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			code = http.StatusUnauthorized
		}
		slog.Warn("service db unauthorized", "op", op, "request_id", requestID, "status", code, "err", err)
		return drs.ImplResponse{Code: code, Body: drs.Error{Msg: "unauthorized", StatusCode: int32(code)}}
	case errors.Is(err, core.ErrNotFound):
		slog.Info("service db not found", "op", op, "request_id", requestID, "status", http.StatusNotFound, "err", err)
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: "not found", StatusCode: http.StatusNotFound}}
	default:
		slog.Error("service db failure", "op", op, "request_id", requestID, "status", http.StatusInternalServerError, "err", err)
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}
	}
}

func unauthorizedStatus(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func forbiddenResponse(ctx context.Context, msg string) drs.ImplResponse {
	code := unauthorizedStatus(ctx)
	slog.Warn("service forbidden", "request_id", core.GetRequestID(ctx), "status", code, "reason", msg)
	return drs.ImplResponse{
		Code: code,
		Body: drs.Error{Msg: msg, StatusCode: int32(code)},
	}
}

func tooLargeResponse(msg string) drs.ImplResponse {
	return drs.ImplResponse{
		Code: http.StatusRequestEntityTooLarge,
		Body: drs.Error{Msg: msg, StatusCode: http.StatusRequestEntityTooLarge},
	}
}

func (s *ObjectsAPIService) GetObject(ctx context.Context, id string, expand bool) (drs.ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetObject", err)
		return resp, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: obj}, nil
}

func (s *ObjectsAPIService) PostObject(ctx context.Context, id string, req drs.PostObjectRequest) (drs.ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	return s.GetObject(ctx, id, false)
}

func (s *ObjectsAPIService) OptionsObject(ctx context.Context, id string) (drs.ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "OptionsObject", err)
		return resp, err
	}
	authz := authorizationsForObject(obj)
	if len(authz.BearerAuthIssuers) == 0 {
		return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: authz}, nil
}

func (s *ObjectsAPIService) DeleteObject(ctx context.Context, id string, req drs.DeleteRequest) (drs.ImplResponse, error) {
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "DeleteObject.GetObject", err)
		return resp, err
	}
	targetResources := obj.Authorizations
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !core.HasMethodAccess(ctx, "delete", targetResources) {
		return forbiddenResponse(ctx, "forbidden: missing delete permission"), nil
	}

	err = s.db.DeleteObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "DeleteObject.DeleteObject", err)
		return resp, err
	}
	return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) BulkDeleteObjects(ctx context.Context, req drs.BulkDeleteRequest) (drs.ImplResponse, error) {
	if len(req.BulkObjectIds) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "bulk_object_ids cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	if len(req.BulkObjectIds) > defaultMaxBulkDeleteLength {
		return tooLargeResponse(fmt.Sprintf("bulk delete request contains %d objects but server maximum is %d", len(req.BulkObjectIds), defaultMaxBulkDeleteLength)), nil
	}

	fetched, err := s.db.GetBulkObjects(ctx, req.BulkObjectIds)
	if err != nil {
		resp := errorResponseForDBError(ctx, "BulkDeleteObjects.GetBulkObjects", err)
		return resp, err
	}
	byID := make(map[string]core.InternalObject, len(fetched))
	for _, obj := range fetched {
		byID[obj.Id] = obj
	}

	for _, id := range req.BulkObjectIds {
		obj, ok := byID[id]
		if !ok {
			resp := errorResponseForDBError(ctx, "BulkDeleteObjects.GetBulkObjects", core.ErrNotFound)
			return resp, core.ErrNotFound
		}
		targetResources := obj.Authorizations
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(ctx, "delete", targetResources) {
			return forbiddenResponse(ctx, "forbidden: missing delete permission"), nil
		}
	}
	if err := s.db.BulkDeleteObjects(ctx, req.BulkObjectIds); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) GetBulkObjects(ctx context.Context, req drs.GetBulkObjectsRequest, expand bool) (drs.ImplResponse, error) {
	if len(req.BulkObjectIds) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "bulk_object_ids cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	if len(req.BulkObjectIds) > defaultMaxBulkRequestLength {
		return tooLargeResponse(fmt.Sprintf("bulk object request contains %d object IDs but server maximum is %d", len(req.BulkObjectIds), defaultMaxBulkRequestLength)), nil
	}

	fetched, err := s.db.GetBulkObjects(ctx, req.BulkObjectIds)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetBulkObjects.GetBulkObjects", err)
		return resp, err
	}
	byID := make(map[string]core.InternalObject, len(fetched))
	for _, obj := range fetched {
		byID[obj.Id] = obj
	}

	resolved := make([]drs.DrsObject, 0, len(req.BulkObjectIds))
	missing := make([]string, 0)
	denied := make([]string, 0)
	for _, id := range req.BulkObjectIds {
		obj, ok := byID[id]
		if !ok {
			slog.Info("bulk get unresolved", "request_id", core.GetRequestID(ctx), "id", id, "err", core.ErrNotFound)
			missing = append(missing, id)
			continue
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
			slog.Warn("bulk get denied by access check", "request_id", core.GetRequestID(ctx), "id", id)
			denied = append(denied, id)
			continue
		}
		resolved = append(resolved, obj.DrsObject)
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

func (s *ObjectsAPIService) OptionsBulkObject(ctx context.Context, _ drs.BulkObjectIdNoPassport) (drs.ImplResponse, error) {
	// In issue-416 spec branch, OPTIONS /objects no longer includes bulk object ids in request payload.
	// Return 204 when object-level authorization introspection is not available via this shape.
	return drs.ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) RegisterObjects(ctx context.Context, req drs.RegisterObjectsRequest) (drs.ImplResponse, error) {
	if len(req.Candidates) == 0 {
		return drs.ImplResponse{
			Code: http.StatusBadRequest,
			Body: drs.Error{Msg: "candidates cannot be empty", StatusCode: http.StatusBadRequest},
		}, nil
	}
	if len(req.Candidates) > defaultMaxRegisterRequestLength {
		return tooLargeResponse(fmt.Sprintf("register request contains %d candidates but server maximum is %d", len(req.Candidates), defaultMaxRegisterRequestLength)), nil
	}

	var objects []core.InternalObject
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
				return forbiddenResponse(ctx, "forbidden: missing file_upload/create permission on /data_file"), nil
			}
		} else if !core.HasMethodAccess(ctx, "create", targetResources) {
			if !core.HasMethodAccess(ctx, "file_upload", []string{"/data_file"}) {
				return forbiddenResponse(ctx, "forbidden: missing create permission"), nil
			}
		}
		objects = append(objects, drsmap.WrapExternal(obj, authz))
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
	out := drsmap.ToExternalMap(objsMap)
	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func (s *ObjectsAPIService) AddChecksums(ctx context.Context, objectID string, checksums []drs.Checksum) (drs.ImplResponse, error) {
	if len(checksums) == 0 {
		return drs.ImplResponse{
			Code: http.StatusBadRequest,
			Body: drs.Error{Msg: "checksums cannot be empty", StatusCode: http.StatusBadRequest},
		}, nil
	}
	if len(checksums) > defaultMaxChecksumAdditionsPerObject {
		return tooLargeResponse(fmt.Sprintf("checksum update contains %d checksums but server maximum is %d", len(checksums), defaultMaxChecksumAdditionsPerObject)), nil
	}

	obj, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "AddChecksums.GetObject", err)
		return resp, err
	}
	targetResources := obj.Authorizations
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !core.HasMethodAccess(ctx, "update", targetResources) {
		return forbiddenResponse(ctx, "forbidden: missing update permission"), nil
	}

	merged := mergeAdditionalChecksums(obj.Checksums, checksums)
	updated := *obj
	updated.Checksums = merged
	updated.UpdatedTime = time.Now()
	if err := s.db.RegisterObjects(ctx, []core.InternalObject{updated}); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	refreshed, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "AddChecksums.GetObjectPostUpdate", err)
		return resp, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: refreshed.DrsObject}, nil
}

func (s *ObjectsAPIService) BulkAddChecksums(ctx context.Context, updatesByID map[string][]drs.Checksum) (drs.ImplResponse, error) {
	if len(updatesByID) == 0 {
		return drs.ImplResponse{
			Code: http.StatusBadRequest,
			Body: drs.Error{Msg: "updates cannot be empty", StatusCode: http.StatusBadRequest},
		}, nil
	}
	if len(updatesByID) > defaultMaxBulkChecksumAdditionLength {
		return tooLargeResponse(fmt.Sprintf("bulk checksum request contains %d updates but server maximum is %d", len(updatesByID), defaultMaxBulkChecksumAdditionLength)), nil
	}

	for objectID, checksums := range updatesByID {
		if strings.TrimSpace(objectID) == "" {
			return drs.ImplResponse{
				Code: http.StatusBadRequest,
				Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest},
			}, nil
		}
		if len(checksums) == 0 {
			return drs.ImplResponse{
				Code: http.StatusBadRequest,
				Body: drs.Error{Msg: "checksums cannot be empty", StatusCode: http.StatusBadRequest},
			}, nil
		}
	}

	// Validate all objects/permissions first to preserve all-or-nothing behavior.
	toWrite := make([]core.InternalObject, 0, len(updatesByID))
	for objectID, checksums := range updatesByID {
		obj, err := s.db.GetObject(ctx, objectID)
		if err != nil {
			resp := errorResponseForDBError(ctx, "BulkAddChecksums.GetObject", err)
			return resp, err
		}
		targetResources := obj.Authorizations
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(ctx, "update", targetResources) {
			return forbiddenResponse(ctx, "forbidden: missing update permission"), nil
		}
		updated := *obj
		updated.Checksums = mergeAdditionalChecksums(obj.Checksums, checksums)
		updated.UpdatedTime = time.Now()
		toWrite = append(toWrite, updated)
	}

	if err := s.db.RegisterObjects(ctx, toWrite); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}

	updatedObjects := make([]drs.DrsObject, 0, len(updatesByID))
	for objectID := range updatesByID {
		obj, err := s.db.GetObject(ctx, objectID)
		if err != nil {
			resp := errorResponseForDBError(ctx, "BulkAddChecksums.GetObjectPostUpdate", err)
			return resp, err
		}
		updatedObjects = append(updatedObjects, obj.DrsObject)
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drs.BulkUpdateAccessMethods200Response{Objects: updatedObjects}}, nil
}

func (s *ObjectsAPIService) GetAccessURL(ctx context.Context, objectID string, accessID string) (drs.ImplResponse, error) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(accessID) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id and access_id are required", StatusCode: http.StatusBadRequest}}, nil
	}
	obj, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetAccessURL.GetObject", err)
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
	if recErr := s.db.RecordFileDownload(ctx, objectID); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(ctx), "object_id", objectID, "err", recErr)
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
	if len(req.BulkObjectAccessIds) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "bulk_object_access_ids cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	if len(req.BulkObjectAccessIds) > defaultMaxBulkRequestLength {
		return tooLargeResponse(fmt.Sprintf("bulk access request contains %d object mappings but server maximum is %d", len(req.BulkObjectAccessIds), defaultMaxBulkRequestLength)), nil
	}

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
	if strings.TrimSpace(objectID) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	if len(req.AccessMethods) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "access_methods cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}

	existing, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "UpdateObjectAccessMethods.GetObject", err)
		return resp, err
	}
	targetResources := existing.Authorizations
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !core.HasMethodAccess(ctx, "update", targetResources) {
		return forbiddenResponse(ctx, "forbidden: missing update permission"), nil
	}

	if err := s.db.UpdateObjectAccessMethods(ctx, objectID, req.AccessMethods); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	updated, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drsmap.ToExternal(*updated)}, nil
}

func (s *ObjectsAPIService) GetObjectsByChecksum(ctx context.Context, checksum string) (drs.ImplResponse, error) {
	objs, err := s.db.GetObjectsByChecksum(ctx, normalizeChecksum(checksum))
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	if len(objs) == 0 {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drs.Error{Msg: "object not found for checksum", StatusCode: http.StatusNotFound}}, nil
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drsmap.ToExternal(objs[0])}, nil
}

func normalizeChecksum(cs string) string {
	if parts := strings.SplitN(cs, ":", 2); len(parts) == 2 {
		return parts[1]
	}
	return cs
}

func normalizeChecksumType(checksumType string) string {
	normalized := strings.ToLower(strings.TrimSpace(checksumType))
	normalized = strings.ReplaceAll(normalized, "-", "")
	return normalized
}

func mergeAdditionalChecksums(existing []drs.Checksum, additions []drs.Checksum) []drs.Checksum {
	out := make([]drs.Checksum, 0, len(existing)+len(additions))
	seenTypes := make(map[string]struct{}, len(existing)+len(additions))

	for _, cs := range existing {
		if t := normalizeChecksumType(cs.Type); t != "" {
			seenTypes[t] = struct{}{}
		}
		out = append(out, cs)
	}

	for _, cs := range additions {
		t := normalizeChecksumType(cs.Type)
		v := strings.TrimSpace(normalizeChecksum(cs.Checksum))
		if t == "" || v == "" {
			continue
		}
		if _, exists := seenTypes[t]; exists {
			// Do not alter existing checksum types.
			continue
		}
		out = append(out, drs.Checksum{Type: strings.TrimSpace(cs.Type), Checksum: v})
		seenTypes[t] = struct{}{}
	}
	return out
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
	if len(req.Updates) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "updates cannot be empty", StatusCode: http.StatusBadRequest}}, nil
	}
	if len(req.Updates) > defaultMaxBulkAccessMethodUpdateLength {
		return tooLargeResponse(fmt.Sprintf("bulk access method update contains %d updates but server maximum is %d", len(req.Updates), defaultMaxBulkAccessMethodUpdateLength)), nil
	}

	updates := make(map[string][]drs.AccessMethod)
	updated := make([]drs.DrsObject, 0, len(req.Updates))
	for _, u := range req.Updates {
		obj, err := s.db.GetObject(ctx, u.ObjectId)
		if err != nil {
			resp := errorResponseForDBError(ctx, "BulkUpdateAccessMethods.GetObject", err)
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
			slog.Warn("bulk access method update post-fetch failed", "request_id", core.GetRequestID(ctx), "object_id", u.ObjectId, "err", err)
			continue
		}
		updated = append(updated, obj.DrsObject)
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

func (s *ObjectsAPIService) PostUploadRequest(ctx context.Context, uploadRequest drs.UploadRequest) (drs.ImplResponse, error) {
	targetResources := []string{"/data_file"}
	if core.IsGen3Mode(ctx) && !core.HasMethodAccess(ctx, "file_upload", targetResources) && !core.HasMethodAccess(ctx, "create", targetResources) {
		code := unauthorizedStatus(ctx)
		return drs.ImplResponse{
			Code: code,
			Body: drs.Error{Msg: "forbidden: missing file_upload/create permission on /data_file", StatusCode: int32(code)},
		}, nil
	}

	creds, err := s.db.ListS3Credentials(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	if len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
		return drs.ImplResponse{
			Code: http.StatusInternalServerError,
			Body: drs.Error{Msg: "no bucket credentials configured for upload", StatusCode: http.StatusInternalServerError},
		}, nil
	}
	bucket := strings.TrimSpace(creds[0].Bucket)
	region := strings.TrimSpace(creds[0].Region)
	if region == "" {
		region = "us-east-1"
	}

	out := drs.UploadResponse{
		Responses: make([]drs.UploadResponseObject, 0, len(uploadRequest.Requests)),
	}
	for i, req := range uploadRequest.Requests {
		key := uploadObjectKey(req, i)
		s3URL := fmt.Sprintf("s3://%s/%s", bucket, key)
		signedURL, signErr := s.urlManager.SignUploadURL(ctx, "", s3URL, urlmanager.SignOptions{})
		if signErr != nil {
			return drs.ImplResponse{
				Code: http.StatusInternalServerError,
				Body: drs.Error{Msg: signErr.Error(), StatusCode: http.StatusInternalServerError},
			}, signErr
		}

		respObj := drs.UploadResponseObject{
			Name:        req.Name,
			Size:        req.Size,
			MimeType:    req.MimeType,
			Checksums:   req.Checksums,
			Description: req.Description,
			Aliases:     req.Aliases,
			UploadMethods: []drs.UploadMethod{
				{
					Type:      "s3",
					AccessUrl: drs.UploadMethodAccessUrl{Url: signedURL},
					Region:    region,
					UploadDetails: map[string]interface{}{
						"bucket": bucket,
						"key":    key,
					},
				},
			},
		}
		out.Responses = append(out.Responses, respObj)
	}

	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func authorizationsForObject(obj *core.DrsObjectWithAuthz) drs.Authorizations {
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

func uploadObjectKey(req drs.UploadRequestObject, index int) string {
	if sha, ok := canonicalSHA256(req.Checksums); ok {
		return "uploads/" + sha
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return fmt.Sprintf("uploads/request-%d-%d", time.Now().UnixNano(), index)
	}
	name = strings.ReplaceAll(name, " ", "_")
	return "uploads/" + name
}
