package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/urlmanager"
)

func (s *ObjectsAPIService) GetAccessURL(ctx context.Context, objectID string, accessID string) (drs.ImplResponse, error) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(accessID) == "" {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drsError("object_id and access_id are required", http.StatusBadRequest)}, nil
	}
	obj, err := s.db.GetObject(ctx, objectID)
	if err != nil {
		resp := errorResponseForDBError(ctx, "GetAccessURL.GetObject", err)
		return resp, err
	}
	if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
		code := unauthorizedStatus(ctx)
		return drs.ImplResponse{Code: code, Body: drsError("unauthorized", code)}, nil
	}

	var selectedAccessMethod *drs.AccessMethod
	if obj.AccessMethods != nil {
		for _, method := range *obj.AccessMethods {
			if core.StringVal(method.AccessId) == accessID {
				selectedAccessMethod = &method
				break
			}
		}
	}

	if selectedAccessMethod == nil {
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drsError("access_id not found", http.StatusNotFound)}, nil
	}

	if selectedAccessMethod.AccessUrl == nil || selectedAccessMethod.AccessUrl.Url == "" {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drsError("access method has no URL", http.StatusInternalServerError)}, nil
	}

	signedURL, err := s.urlManager.SignURL(ctx, accessID, selectedAccessMethod.AccessUrl.Url, urlmanager.SignOptions{})
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	if recErr := s.db.RecordFileDownload(ctx, objectID); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(ctx), "object_id", objectID, "err", recErr)
	}

	return drs.ImplResponse{
		Code: http.StatusOK,
		Body: drs.AccessURL{
			Url: signedURL,
		},
	}, nil
}

func (s *ObjectsAPIService) PostAccessURL(ctx context.Context, objectID string, accessID string, req drs.PostAccessURLRequestObject) (drs.ImplResponse, error) {
	return s.GetAccessURL(ctx, objectID, accessID)
}

func (s *ObjectsAPIService) GetBulkAccessURL(ctx context.Context, req drs.BulkObjectAccessId) (drs.ImplResponse, error) {
	if req.BulkObjectAccessIds == nil || len(*req.BulkObjectAccessIds) == 0 {
		return drs.ImplResponse{Code: http.StatusBadRequest, Body: drsError("bulk_object_access_ids cannot be empty", http.StatusBadRequest)}, nil
	}
	if len(*req.BulkObjectAccessIds) > defaultMaxBulkRequestLength {
		return tooLargeResponse(fmt.Sprintf("bulk access request contains %d object mappings but server maximum is %d", len(*req.BulkObjectAccessIds), defaultMaxBulkRequestLength)), nil
	}

	var results []drs.BulkAccessUrl
	unresolvedByCode := map[int32][]string{}
	requested := 0
	for _, mapping := range *req.BulkObjectAccessIds {
		if mapping.BulkAccessIds == nil {
			continue
		}
		for _, accessID := range *mapping.BulkAccessIds {
			requested++
			resp, err := s.GetAccessURL(ctx, core.StringVal(mapping.BulkObjectId), accessID)
			if err != nil || resp.Code != http.StatusOK {
				code := int32(resp.Code)
				if code == 0 {
					code = http.StatusInternalServerError
				}
				unresolvedByCode[code] = append(unresolvedByCode[code], core.StringVal(mapping.BulkObjectId))
				continue
			}
			accessURL, ok := resp.Body.(drs.AccessURL)
			if !ok {
				unresolvedByCode[http.StatusInternalServerError] = append(unresolvedByCode[http.StatusInternalServerError], core.StringVal(mapping.BulkObjectId))
				continue
			}
			drsObjectID := core.StringVal(mapping.BulkObjectId)
			accessIDCopy := accessID
			results = append(results, drs.BulkAccessUrl{
				DrsObjectId: &drsObjectID,
				DrsAccessId: &accessIDCopy,
				Url:         accessURL.Url,
				Headers:     accessURL.Headers,
			})
		}
	}
	requestedCount := requested
	resolvedCount := len(results)
	unresolvedCount := 0
	out := drs.GetBulkAccessUrl200Response{
		Summary: &drs.Summary{
			Requested:  &requestedCount,
			Resolved:   &resolvedCount,
			Unresolved: &unresolvedCount,
		},
		ResolvedDrsObjectAccessUrls: &results,
	}
	unresolved := drs.Unresolved{}
	for code, ids := range unresolvedByCode {
		uniq := uniqueStrings(ids)
		unresolvedCount += len(uniq)
		codeCopy := int(code)
		unresolved = append(unresolved, struct {
			ErrorCode *int      `json:"error_code,omitempty"`
			ObjectIds *[]string `json:"object_ids,omitempty"`
		}{
			ErrorCode: &codeCopy,
			ObjectIds: &uniq,
		})
	}
	out.UnresolvedDrsObjects = &unresolved
	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}
