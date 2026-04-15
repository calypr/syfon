package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/db/core"
)

var _ drs.StrictServerInterface = (*StrictServer)(nil)

type StrictServer struct {
	svc *ObjectsAPIService
}

func NewStrictServer(svc *ObjectsAPIService) *StrictServer {
	return &StrictServer{svc: svc}
}

func convertType[T any](in any) (T, error) {
	var out T
	if in == nil {
		return out, nil
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return out, fmt.Errorf("marshal conversion body: %w", err)
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("unmarshal conversion body: %w", err)
	}
	return out, nil
}

func makeError(msg string, status int) drs.Error {
	return drsError(msg, status,)
}

func errorBody(resp drs.ImplResponse, err error) drs.Error {
	if e, ok := resp.Body.(drs.Error); ok {
		return e
	}
	if e, ok := resp.Body.(*drs.Error); ok && e != nil {
		return *e
	}
	if resp.Body != nil {
		if converted, convErr := convertType[drs.Error](resp.Body); convErr == nil {
			if core.StringVal(converted.Msg) != "" || core.IntVal(converted.StatusCode) != 0 {
				return converted
			}
		}
	}

	status := resp.Code
	if status == 0 {
		status = http.StatusInternalServerError
	}
	msg := http.StatusText(status)
	if err != nil {
		msg = err.Error()
	}
	if msg == "" {
		msg = "internal server error"
	}
	return drsError(msg, status)
}

func asCode(resp drs.ImplResponse, err error) int {
	if resp.Code != 0 {
		return resp.Code
	}
	if err != nil {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

func (s *StrictServer) OptionsBulkObject(ctx context.Context, request drs.OptionsBulkObjectRequestObject) (drs.OptionsBulkObjectResponseObject, error) {
	var in drs.BulkObjectIdNoPassport
	if request.Body != nil {
		converted, err := convertType[drs.BulkObjectIdNoPassport](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.OptionsBulkObject400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}

	resp, err := s.svc.OptionsBulkObject(ctx, in)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkBulkAuthorizationsJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.OptionsBulkObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.OptionsBulkObject200JSONResponse{N200OkBulkAuthorizationsJSONResponse: body}, nil
	case http.StatusNoContent:
		return drs.OptionsBulkObject204Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.OptionsBulkObject400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.OptionsBulkObject404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	case http.StatusMethodNotAllowed:
		return drs.OptionsBulkObject405Response{}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.OptionsBulkObject413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.OptionsBulkObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetBulkObjects(ctx context.Context, request drs.GetBulkObjectsRequestObject) (drs.GetBulkObjectsResponseObject, error) {
	resp, err := s.svc.GetBulkObjects(ctx, request, request.Params.Expand != nil && *request.Params.Expand)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkDrsObjectsJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetBulkObjects500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetBulkObjects200JSONResponse{N200OkDrsObjectsJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.GetBulkObjects202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.GetBulkObjects400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.GetBulkObjects401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.GetBulkObjects403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.GetBulkObjects404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.GetBulkObjects413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetBulkObjects500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetBulkAccessURL(ctx context.Context, request drs.GetBulkAccessURLRequestObject) (drs.GetBulkAccessURLResponseObject, error) {
	var in drs.BulkObjectAccessId
	if request.Body != nil {
		converted, err := convertType[drs.BulkObjectAccessId](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.GetBulkAccessURL400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.GetBulkAccessURL(ctx, in)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkAccessesJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetBulkAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetBulkAccessURL200JSONResponse{N200OkAccessesJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.GetBulkAccessURL202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL404JSONResponse{N404NotFoundAccessJSONResponse: drs.N404NotFoundAccessJSONResponse(e)}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetBulkAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) BulkUpdateAccessMethods(ctx context.Context, request drs.BulkUpdateAccessMethodsRequestObject) (drs.BulkUpdateAccessMethodsResponseObject, error) {
	var in drs.BulkAccessMethodUpdateRequest
	if request.Body != nil {
		converted, err := convertType[drs.BulkAccessMethodUpdateRequest](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.BulkUpdateAccessMethods400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.BulkUpdateAccessMethods(ctx, in)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200BulkAccessMethodUpdateJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.BulkUpdateAccessMethods500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.BulkUpdateAccessMethods200JSONResponse{N200BulkAccessMethodUpdateJSONResponse: body}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.BulkUpdateAccessMethods500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetObjectsByChecksum(ctx context.Context, request drs.GetObjectsByChecksumRequestObject) (drs.GetObjectsByChecksumResponseObject, error) {
	resp, err := s.svc.GetObjectsByChecksum(ctx, string(request.ChecksumParam))
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkDrsObjectsJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetObjectsByChecksum500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetObjectsByChecksum200JSONResponse{N200OkDrsObjectsJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.GetObjectsByChecksum202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.GetObjectsByChecksum400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.GetObjectsByChecksum401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.GetObjectsByChecksum403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.GetObjectsByChecksum404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetObjectsByChecksum500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) BulkDeleteObjects(ctx context.Context, request drs.BulkDeleteObjectsRequestObject) (drs.BulkDeleteObjectsResponseObject, error) {
	var in drs.BulkDeleteRequest
	if request.Body != nil {
		converted, err := convertType[drs.BulkDeleteRequest](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.BulkDeleteObjects400JSONResponse{N400BadRequestDeleteJSONResponse: drs.N400BadRequestDeleteJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.BulkDeleteObjects(ctx, in)
	switch asCode(resp, err) {
	case http.StatusNoContent:
		return drs.BulkDeleteObjects204Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects400JSONResponse{N400BadRequestDeleteJSONResponse: drs.N400BadRequestDeleteJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects403JSONResponse{N403ForbiddenDeleteJSONResponse: drs.N403ForbiddenDeleteJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects404JSONResponse{N404NotFoundDeleteJSONResponse: drs.N404NotFoundDeleteJSONResponse(e)}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.BulkDeleteObjects500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) RegisterObjects(ctx context.Context, request drs.RegisterObjectsRequestObject) (drs.RegisterObjectsResponseObject, error) {
	var in drs.RegisterObjectsBody
	if request.Body != nil {
		in = drs.RegisterObjectsBody{
			Candidates: request.Body.Candidates,
			Passports:  request.Body.Passports,
		}
	}
	resp, err := s.svc.RegisterObjects(ctx, in)
	switch asCode(resp, err) {
	case http.StatusCreated:
		body, convErr := convertType[drs.N201ObjectsCreatedJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.RegisterObjects500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.RegisterObjects201JSONResponse{N201ObjectsCreatedJSONResponse: body}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.RegisterObjects400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.RegisterObjects401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.RegisterObjects403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusRequestEntityTooLarge:
		e := errorBody(resp, err)
		return drs.RegisterObjects413JSONResponse{N413RequestTooLargeJSONResponse: drs.N413RequestTooLargeJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.RegisterObjects500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetObject(ctx context.Context, request drs.GetObjectRequestObject) (drs.GetObjectResponseObject, error) {
	resp, err := s.svc.GetObject(ctx, string(request.ObjectId), request.Params.Expand != nil && *request.Params.Expand)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkDrsObjectJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetObject200JSONResponse{N200OkDrsObjectJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.GetObject202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.GetObject400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.GetObject401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.GetObject403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.GetObject404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) OptionsObject(ctx context.Context, request drs.OptionsObjectRequestObject) (drs.OptionsObjectResponseObject, error) {
	resp, err := s.svc.OptionsObject(ctx, string(request.ObjectId))
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkAuthorizationsJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.OptionsObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.OptionsObject200JSONResponse{N200OkAuthorizationsJSONResponse: body}, nil
	case http.StatusNoContent:
		return drs.OptionsObject204Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.OptionsObject400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.OptionsObject404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	case http.StatusMethodNotAllowed:
		return drs.OptionsObject405Response{}, nil
	default:
		e := errorBody(resp, err)
		return drs.OptionsObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) PostObject(ctx context.Context, request drs.PostObjectRequestObject) (drs.PostObjectResponseObject, error) {
	resp, err := s.svc.PostObject(ctx, string(request.ObjectId), request)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkDrsObjectJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.PostObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.PostObject200JSONResponse{N200OkDrsObjectJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.PostObject202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.PostObject400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.PostObject401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.PostObject403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.PostObject404JSONResponse{N404NotFoundAccessJSONResponse: drs.N404NotFoundAccessJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.PostObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) UpdateObjectAccessMethods(ctx context.Context, request drs.UpdateObjectAccessMethodsRequestObject) (drs.UpdateObjectAccessMethodsResponseObject, error) {
	var in drs.AccessMethodUpdateRequest
	if request.Body != nil {
		converted, err := convertType[drs.AccessMethodUpdateRequest](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.UpdateObjectAccessMethods400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.UpdateObjectAccessMethods(ctx, request.ObjectId, in)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200AccessMethodUpdateJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.UpdateObjectAccessMethods500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.UpdateObjectAccessMethods200JSONResponse{N200AccessMethodUpdateJSONResponse: body}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.UpdateObjectAccessMethods400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.UpdateObjectAccessMethods401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.UpdateObjectAccessMethods403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.UpdateObjectAccessMethods404JSONResponse{N404NotFoundDrsObjectJSONResponse: drs.N404NotFoundDrsObjectJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.UpdateObjectAccessMethods500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetAccessURL(ctx context.Context, request drs.GetAccessURLRequestObject) (drs.GetAccessURLResponseObject, error) {
	resp, err := s.svc.GetAccessURL(ctx, string(request.ObjectId), string(request.AccessId))
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkAccessJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetAccessURL200JSONResponse{N200OkAccessJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.GetAccessURL202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.GetAccessURL400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.GetAccessURL401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.GetAccessURL403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.GetAccessURL404JSONResponse{N404NotFoundAccessJSONResponse: drs.N404NotFoundAccessJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) PostAccessURL(ctx context.Context, request drs.PostAccessURLRequestObject) (drs.PostAccessURLResponseObject, error) {
	var in drs.PostAccessURLJSONRequestBody
	if request.Body != nil {
		converted, err := convertType[drs.PostAccessURLJSONRequestBody](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.PostAccessURL400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}
	request.Body = &in
	resp, err := s.svc.PostAccessURL(ctx, string(request.ObjectId), string(request.AccessId), request)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200OkAccessJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.PostAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.PostAccessURL200JSONResponse{N200OkAccessJSONResponse: body}, nil
	case http.StatusAccepted:
		return drs.PostAccessURL202Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.PostAccessURL400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.PostAccessURL401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.PostAccessURL403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.PostAccessURL404JSONResponse{N404NotFoundAccessJSONResponse: drs.N404NotFoundAccessJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.PostAccessURL500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) DeleteObject(ctx context.Context, request drs.DeleteObjectRequestObject) (drs.DeleteObjectResponseObject, error) {
	var in drs.DeleteRequest
	if request.Body != nil {
		converted, err := convertType[drs.DeleteRequest](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.DeleteObject400JSONResponse{N400BadRequestDeleteJSONResponse: drs.N400BadRequestDeleteJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.DeleteObject(ctx, string(request.ObjectId), in)
	switch asCode(resp, err) {
	case http.StatusNoContent:
		return drs.DeleteObject204Response{}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.DeleteObject400JSONResponse{N400BadRequestDeleteJSONResponse: drs.N400BadRequestDeleteJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.DeleteObject401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.DeleteObject403JSONResponse{N403ForbiddenDeleteJSONResponse: drs.N403ForbiddenDeleteJSONResponse(e)}, nil
	case http.StatusNotFound:
		e := errorBody(resp, err)
		return drs.DeleteObject404JSONResponse{N404NotFoundDeleteJSONResponse: drs.N404NotFoundDeleteJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.DeleteObject500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) GetServiceInfo(ctx context.Context, _ drs.GetServiceInfoRequestObject) (drs.GetServiceInfoResponseObject, error) {
	resp, err := s.svc.GetServiceInfo(ctx)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200ServiceInfoJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.GetServiceInfo500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.GetServiceInfo200JSONResponse{N200ServiceInfoJSONResponse: body}, nil
	default:
		e := errorBody(resp, err)
		return drs.GetServiceInfo500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}

func (s *StrictServer) PostUploadRequest(ctx context.Context, request drs.PostUploadRequestRequestObject) (drs.PostUploadRequestResponseObject, error) {
	var in drs.UploadRequest
	if request.Body != nil {
		converted, err := convertType[drs.UploadRequest](*request.Body)
		if err != nil {
			e := makeError(err.Error(), http.StatusBadRequest)
			return drs.PostUploadRequest400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
		}
		in = converted
	}
	resp, err := s.svc.PostUploadRequest(ctx, in)
	switch asCode(resp, err) {
	case http.StatusOK:
		body, convErr := convertType[drs.N200UploadRequestJSONResponse](resp.Body)
		if convErr != nil {
			e := errorBody(resp, convErr)
			return drs.PostUploadRequest500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
		}
		return drs.PostUploadRequest200JSONResponse{N200UploadRequestJSONResponse: body}, nil
	case http.StatusBadRequest:
		e := errorBody(resp, err)
		return drs.PostUploadRequest400JSONResponse{N400BadRequestJSONResponse: drs.N400BadRequestJSONResponse(e)}, nil
	case http.StatusUnauthorized:
		e := errorBody(resp, err)
		return drs.PostUploadRequest401JSONResponse{N401UnauthorizedJSONResponse: drs.N401UnauthorizedJSONResponse(e)}, nil
	case http.StatusForbidden:
		e := errorBody(resp, err)
		return drs.PostUploadRequest403JSONResponse{N403ForbiddenJSONResponse: drs.N403ForbiddenJSONResponse(e)}, nil
	default:
		e := errorBody(resp, err)
		return drs.PostUploadRequest500JSONResponse{N500InternalServerErrorJSONResponse: drs.N500InternalServerErrorJSONResponse(e)}, nil
	}
}
