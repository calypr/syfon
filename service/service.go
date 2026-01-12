package service

import (
	"context"
	"errors"
	"net/http"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db"
)

type ObjectsAPIService struct {
	db db.DatabaseInterface
}

func NewObjectsAPIService(db db.DatabaseInterface) *ObjectsAPIService {
	return &ObjectsAPIService{db: db}
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
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) GetAccessURL(ctx context.Context, objectID string, accessID string) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) PostAccessURL(ctx context.Context, objectID string, accessID string, req drs.PostAccessUrlRequest) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) GetBulkAccessURL(ctx context.Context, req drs.BulkObjectAccessId) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) UpdateObjectAccessMethods(ctx context.Context, objectID string, req drs.AccessMethodUpdateRequest) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) GetObjectsByChecksum(ctx context.Context, checksum string) (drs.ImplResponse, error) {
	objs, err := s.db.GetObjectsByChecksum(ctx, checksum)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: objs}, nil
}

func (s *ObjectsAPIService) BulkUpdateAccessMethods(ctx context.Context, req drs.BulkAccessMethodUpdateRequest) (drs.ImplResponse, error) {
	return drs.ImplResponse{Code: http.StatusNotImplemented, Body: nil}, errors.New("method not implemented")
}

func (s *ObjectsAPIService) GetServiceInfo(ctx context.Context) (drs.ImplResponse, error) {
	info, err := s.db.GetServiceInfo(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: info}, nil
}
