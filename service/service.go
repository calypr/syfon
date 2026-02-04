package service

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/google/uuid"
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
	var objects []drs.DrsObject
	now := time.Now()

	for _, c := range req.Candidates {
		id := uuid.New().String()
		obj := drs.DrsObject{
			Id:          id,
			Name:        c.Name,
			Size:        c.Size,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
			Description: c.Description,
			Aliases:     c.Aliases,
			Checksums:   c.Checksums,
			SelfUri:     "drs://" + id,
		}

		// Convert candidates access methods to drs objects access methods
		for _, am := range c.AccessMethods {
			obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
				Type:      am.Type,
				AccessUrl: am.AccessUrl,
				Region:    am.Region,
			})
		}
		objects = append(objects, obj)
	}

	if err := s.db.RegisterObjects(ctx, objects); err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}

	return drs.ImplResponse{Code: http.StatusOK, Body: objects}, nil
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
	objs, err := s.db.GetObjectsByChecksum(ctx, checksum)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: objs}, nil
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
