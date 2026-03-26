package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

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
