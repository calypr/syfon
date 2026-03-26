package service

import (
	"context"
	"net/http"

	"github.com/calypr/drs-server/apigen/drs"
)

func (s *ObjectsAPIService) GetServiceInfo(ctx context.Context) (drs.ImplResponse, error) {
	info, err := s.db.GetServiceInfo(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	return drs.ImplResponse{Code: http.StatusOK, Body: info}, nil
}
