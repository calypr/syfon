package service

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/adapter/drsmap"
	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

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
			missing = append(missing, id)
			continue
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
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
		authz := make([]string, 0)

		obj := drs.DrsObject{
			Name:        c.Name,
			Size:        c.Size,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
			Description: c.Description,
			Aliases:     c.Aliases,
		}
		obj.Checksums = []drs.Checksum{{Type: "sha256", Checksum: primaryChecksum}}

		seenAccess := make(map[string]struct{})
		seenAuthz := make(map[string]struct{})
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
		id := core.MintObjectIDFromChecksum(primaryChecksum, authz)
		obj.Id = id
		obj.SelfUri = "drs://" + id

		targetResources := authz
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
			return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "object_id cannot be empty", StatusCode: http.StatusBadRequest}}, nil
		}
		if len(checksums) == 0 {
			return drs.ImplResponse{Code: http.StatusBadRequest, Body: drs.Error{Msg: "checksums cannot be empty", StatusCode: http.StatusBadRequest}}, nil
		}
	}

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
			continue
		}
		updatedObjects = append(updatedObjects, obj.DrsObject)
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drs.BulkUpdateAccessMethods200Response{Objects: updatedObjects}}, nil
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
			return drs.ImplResponse{Code: code, Body: drs.Error{Msg: "forbidden: missing update permission", StatusCode: int32(code)}}, nil
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
		updated = append(updated, obj.DrsObject)
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: drs.BulkUpdateAccessMethods200Response{Objects: updated}}, nil
}
