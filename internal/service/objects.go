package service

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/adapter/drsmap"
	"github.com/calypr/syfon/internal/db/core"
)

func (s *ObjectsAPIService) GetObject(ctx context.Context, id string, expand bool) (ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id cannot be empty", http.StatusBadRequest)}, nil
	}
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetObject", err)
		return resp, err
	}
	return ImplResponse{Code: http.StatusOK, Body: obj}, nil
}

func (s *ObjectsAPIService) PostObject(ctx context.Context, id string, req drs.PostObjectRequestObject) (ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id cannot be empty", http.StatusBadRequest)}, nil
	}
	return s.GetObject(ctx, id, false)
}

func (s *ObjectsAPIService) OptionsObject(ctx context.Context, id string) (ImplResponse, error) {
	if strings.TrimSpace(id) == "" {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id cannot be empty", http.StatusBadRequest)}, nil
	}
	obj, err := s.db.GetObject(ctx, id)
	if err != nil {
		resp := errorResponseForDBError(ctx, "OptionsObject", err)
		return resp, err
	}
	authz := authorizationsForObject(obj)
	if authz.BearerAuthIssuers == nil || len(*authz.BearerAuthIssuers) == 0 {
		return ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
	}
	return ImplResponse{Code: http.StatusOK, Body: authz}, nil
}

func (s *ObjectsAPIService) DeleteObject(ctx context.Context, id string, req drs.DeleteRequest) (ImplResponse, error) {
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
	return ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) BulkDeleteObjects(ctx context.Context, req drs.BulkDeleteRequest) (ImplResponse, error) {
	if len(req.BulkObjectIds) == 0 {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("bulk_object_ids cannot be empty", http.StatusBadRequest)}, nil
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
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	return ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) GetBulkObjects(ctx context.Context, req drs.GetBulkObjectsRequestObject, expand bool) (ImplResponse, error) {
	if req.Body == nil || len(req.Body.BulkObjectIds) == 0 {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("bulk_object_ids cannot be empty", http.StatusBadRequest)}, nil
	}
	if len(req.Body.BulkObjectIds) > defaultMaxBulkRequestLength {
		return tooLargeResponse(fmt.Sprintf("bulk object request contains %d object IDs but server maximum is %d", len(req.Body.BulkObjectIds), defaultMaxBulkRequestLength)), nil
	}

	fetched, err := s.db.GetBulkObjects(ctx, req.Body.BulkObjectIds)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetBulkObjects.GetBulkObjects", err)
		return resp, err
	}
	byID := make(map[string]core.InternalObject, len(fetched))
	for _, obj := range fetched {
		byID[obj.Id] = obj
	}

	resolved := make([]drs.DrsObject, 0, len(req.Body.BulkObjectIds))
	missing := make([]string, 0)
	denied := make([]string, 0)
	for _, id := range req.Body.BulkObjectIds {
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
	out := drs.N200OkDrsObjectsJSONResponse{
		Summary: &drs.Summary{
			Requested:  core.Ptr(len(req.Body.BulkObjectIds)),
			Resolved:   core.Ptr(len(resolved)),
			Unresolved: core.Ptr(len(missing)),
		},
		ResolvedDrsObject: &resolved,
	}
	if len(denied) > 0 {
		code := unauthorizedStatus(ctx)
		ids := uniqueStrings(denied)
		out.UnresolvedDrsObjects = &[]struct {
			ErrorCode *int      `json:"error_code,omitempty"`
			ObjectIds *[]string `json:"object_ids,omitempty"`
		}{
			{
				ErrorCode: &code,
				ObjectIds: &ids,
			},
		}
	}
	if len(missing) > 0 {
		code := http.StatusNotFound
		ids := uniqueStrings(missing)
		if out.UnresolvedDrsObjects == nil {
			out.UnresolvedDrsObjects = &[]struct {
				ErrorCode *int      `json:"error_code,omitempty"`
				ObjectIds *[]string `json:"object_ids,omitempty"`
			}{}
		}
		*out.UnresolvedDrsObjects = append(*out.UnresolvedDrsObjects, struct {
			ErrorCode *int      `json:"error_code,omitempty"`
			ObjectIds *[]string `json:"object_ids,omitempty"`
		}{
			ErrorCode: &code,
			ObjectIds: &ids,
		})
	}
	return ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func (s *ObjectsAPIService) OptionsBulkObject(ctx context.Context, _ drs.BulkObjectIdNoPassport) (ImplResponse, error) {
	return ImplResponse{Code: http.StatusNoContent, Body: nil}, nil
}

func (s *ObjectsAPIService) RegisterObjects(ctx context.Context, req drs.RegisterObjectsBody) (ImplResponse, error) {
	if len(req.Candidates) == 0 {
		return ImplResponse{
			Code: http.StatusBadRequest,
			Body: drsError("candidates cannot be empty", http.StatusBadRequest),
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
			return ImplResponse{
				Code: http.StatusBadRequest,
				Body: drsError("candidate["+strconv.Itoa(i)+"] must include a sha256 checksum", http.StatusBadRequest),
			}, nil
		}
		authz := make([]string, 0)

		obj := drs.DrsObject{
			Name:        c.Name,
			Size:        c.Size,
			CreatedTime: now,
			UpdatedTime: core.Ptr(now),
			Version:     core.Ptr("1"),
			Description: c.Description,
			Aliases:     c.Aliases,
		}
		obj.Checksums = []drs.Checksum{{Type: "sha256", Checksum: primaryChecksum}}

		seenAccess := make(map[string]struct{})
		seenAuthz := make(map[string]struct{})
		var accessMethods []drs.AccessMethod
		if c.AccessMethods != nil {
			accessMethods = make([]drs.AccessMethod, 0, len(*c.AccessMethods))
			for _, am := range *c.AccessMethods {
				if am.AccessUrl == nil || am.AccessUrl.Url == "" {
					continue
				}
				accessKey := string(am.Type) + "|" + am.AccessUrl.Url
				if _, exists := seenAccess[accessKey]; exists {
					continue
				}
				seenAccess[accessKey] = struct{}{}

				accessID := am.AccessId
				if accessID == nil || *accessID == "" {
					accessID = core.Ptr(string(am.Type))
				}
				accessMethods = append(accessMethods, drs.AccessMethod{
					Type:      am.Type,
					AccessUrl: am.AccessUrl,
					AccessId:  accessID,
					Region:    am.Region,
				})
				if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
					for _, issuer := range *am.Authorizations.BearerAuthIssuers {
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
			}
		}
		for i := range accessMethods {
			accessMethods[i].Authorizations = &struct {
				BearerAuthIssuers   *[]string                                       `json:"bearer_auth_issuers,omitempty"`
				DrsObjectId         *string                                         `json:"drs_object_id,omitempty"`
				PassportAuthIssuers *[]string                                       `json:"passport_auth_issuers,omitempty"`
				SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
			}{
				BearerAuthIssuers: &authz,
			}
		}
		obj.AccessMethods = &accessMethods
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
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}

	return ImplResponse{
		Code: http.StatusCreated,
		Body: drs.N201ObjectsCreatedJSONResponse{
			Objects: registered,
		},
	}, nil
}

func (s *ObjectsAPIService) GetObjectsByChecksums(ctx context.Context, checksums []string) (ImplResponse, error) {
	lookupValues := make([]string, len(checksums))
	lookupTypes := make([]string, len(checksums))
	for i := range checksums {
		lookupTypes[i], lookupValues[i] = parseChecksumQuery(checksums[i])
	}

	objsMap, err := s.db.GetObjectsByChecksums(ctx, lookupValues)
	if err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}

	filtered := make(map[string][]core.InternalObject, len(checksums))
	for i := range checksums {
		rawKey := checksums[i]
		valueKey := lookupValues[i]
		objs := objsMap[valueKey]
		if lookupTypes[i] != "" {
			typed := make([]core.InternalObject, 0, len(objs))
			for _, obj := range objs {
				for _, cs := range obj.Checksums {
					if normalizeChecksumType(cs.Type) == lookupTypes[i] && normalizeChecksum(cs.Checksum) == valueKey {
						typed = append(typed, obj)
						break
					}
				}
			}
			objs = typed
		}
		filtered[rawKey] = objs
	}

	out := drsmap.ToExternalMap(filtered)
	return ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func (s *ObjectsAPIService) AddChecksums(ctx context.Context, objectID string, checksums []drs.Checksum) (ImplResponse, error) {
	if len(checksums) == 0 {
		return ImplResponse{
			Code: http.StatusBadRequest,
			Body: drsError("checksums cannot be empty", http.StatusBadRequest),
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
	updated.UpdatedTime = core.Ptr(time.Now())
	if err := s.db.RegisterObjects(ctx, []core.InternalObject{updated}); err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	refreshed, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "AddChecksums.GetObjectPostUpdate", err)
		return resp, err
	}
	return ImplResponse{Code: http.StatusOK, Body: refreshed.DrsObject}, nil
}

func (s *ObjectsAPIService) BulkAddChecksums(ctx context.Context, updatesByID map[string][]drs.Checksum) (ImplResponse, error) {
	if len(updatesByID) == 0 {
		return ImplResponse{
			Code: http.StatusBadRequest,
			Body: drsError("updates cannot be empty", http.StatusBadRequest),
		}, nil
	}
	if len(updatesByID) > defaultMaxBulkChecksumAdditionLength {
		return tooLargeResponse(fmt.Sprintf("bulk checksum request contains %d updates but server maximum is %d", len(updatesByID), defaultMaxBulkChecksumAdditionLength)), nil
	}

	for objectID, checksums := range updatesByID {
		if strings.TrimSpace(objectID) == "" {
			return ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id cannot be empty", http.StatusBadRequest)}, nil
		}
		if len(checksums) == 0 {
			return ImplResponse{Code: http.StatusBadRequest, Body: drsError("checksums cannot be empty", http.StatusBadRequest)}, nil
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
		updated.UpdatedTime = core.Ptr(time.Now())
		toWrite = append(toWrite, updated)
	}

	if err := s.db.RegisterObjects(ctx, toWrite); err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}

	updatedObjects := make([]drs.DrsObject, 0, len(updatesByID))
	for objectID := range updatesByID {
		obj, err := s.db.GetObject(ctx, objectID)
		if err != nil {
			continue
		}
		updatedObjects = append(updatedObjects, obj.DrsObject)
	}
	return ImplResponse{Code: http.StatusOK, Body: drs.N200BulkAccessMethodUpdateJSONResponse{Objects: updatedObjects}}, nil
}

func (s *ObjectsAPIService) UpdateObjectAccessMethods(ctx context.Context, objectID string, req drs.AccessMethodUpdateRequest) (ImplResponse, error) {
	if strings.TrimSpace(objectID) == "" {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id cannot be empty", http.StatusBadRequest)}, nil
	}
	if len(req.AccessMethods) == 0 {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("access_methods cannot be empty", http.StatusBadRequest)}, nil
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
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	updated, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	return ImplResponse{Code: http.StatusOK, Body: drsmap.ToExternal(*updated)}, nil
}

func (s *ObjectsAPIService) GetObjectsByChecksum(ctx context.Context, checksum string) (ImplResponse, error) {
	checksumType, checksumValue := parseChecksumQuery(checksum)
	objs, err := s.db.GetObjectsByChecksum(ctx, checksumValue)
	if err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	if checksumType != "" {
		filtered := make([]core.InternalObject, 0, len(objs))
		for _, obj := range objs {
			for _, cs := range obj.Checksums {
				if normalizeChecksumType(cs.Type) == checksumType && normalizeChecksum(cs.Checksum) == checksumValue {
					filtered = append(filtered, obj)
					break
				}
			}
		}
		objs = filtered
	}
	if len(objs) == 0 {
		return ImplResponse{Code: http.StatusNotFound, Body: drsError("object not found for checksum", http.StatusNotFound)}, nil
	}
	return ImplResponse{Code: http.StatusOK, Body: drsmap.ToExternal(objs[0])}, nil
}

func (s *ObjectsAPIService) BulkUpdateAccessMethods(ctx context.Context, req drs.BulkAccessMethodUpdateRequest) (ImplResponse, error) {
	if len(req.Updates) == 0 {
		return ImplResponse{Code: http.StatusBadRequest, Body: drsError("updates cannot be empty", http.StatusBadRequest)}, nil
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
			return ImplResponse{Code: code, Body: drsError("forbidden: missing update permission", code)}, nil
		}
	}
	for _, update := range req.Updates {
		updates[update.ObjectId] = update.AccessMethods
	}
	if err := s.db.BulkUpdateAccessMethods(ctx, updates); err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	for _, u := range req.Updates {
		obj, err := s.db.GetObject(ctx, u.ObjectId)
		if err != nil {
			continue
		}
		updated = append(updated, obj.DrsObject)
	}
	return ImplResponse{Code: http.StatusOK, Body: drs.N200BulkAccessMethodUpdateJSONResponse{Objects: updated}}, nil
}
