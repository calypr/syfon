package service

import (
	"context"
	"net/http"
)

func (s *ObjectsAPIService) GetServiceInfo(ctx context.Context) (ImplResponse, error) {
	info, err := s.db.GetServiceInfo(ctx)
	if err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	return ImplResponse{Code: http.StatusOK, Body: info}, nil
}
