package drs

import (
	"context"
	"net/http"
	"strings"
)

// ImplResponse preserves the old openapi-generator service contract so the
// service layer can remain stable while the HTTP runtime moves to oapi-codegen.
type ImplResponse struct {
	Code int
	Body interface{}
}

// Response is the compatibility helper used by the legacy service methods.
func Response(code int, body interface{}) ImplResponse {
	return ImplResponse{Code: code, Body: body}
}

// Logger preserves the old HTTP middleware helper used by internal route
// registration while the runtime is now Gin-based.
func Logger(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.ServeHTTP(w, r)
	})
}

// Legacy type aliases preserved for callers that still use the old generator
// names in tests and service code.
type (
	AccessMethodAccessUrl = AccessURL
	UploadMethodAccessUrl = AccessURL
	BulkAccessUrl         = BulkAccessURL
	PostAccessUrlRequestObject = PostAccessURLRequestObject
	RegisterObjectsRequest = RegisterObjectsBody
	GetBulkObjects200Response = N200OkDrsObjectsJSONResponse
	GetBulkAccessUrl200Response = N200OkAccessesJSONResponse
	BulkUpdateAccessMethods200Response = N200BulkAccessMethodUpdateJSONResponse
	RegisterObjects201Response = N201ObjectsCreatedJSONResponse
	GetServiceInfo200Response = N200ServiceInfoJSONResponse
)

// AccessMethodAuthorizations preserves the legacy generated name used by some
// tests and service code while the schema now uses an inline anonymous struct.
type AccessMethodAuthorizations struct {
	BearerAuthIssuers   *[]string `json:"bearer_auth_issuers,omitempty"`
	DrsObjectId         *string   `json:"drs_object_id,omitempty"`
	PassportAuthIssuers *[]string `json:"passport_auth_issuers,omitempty"`
	SupportedTypes      *[]AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
}

// makeError creates the compatibility error body used by legacy service code.
func makeError(msg string, status int) Error {
	trimmed := strings.TrimSpace(msg)
	return Error{Msg: &trimmed, StatusCode: &status}
}

// toUnresolved converts legacy unresolved entries into the oapi generated
// response shape.
func toUnresolved(entries []UnresolvedInner) Unresolved {
	out := make(Unresolved, 0, len(entries))
	for _, entry := range entries {
		entryCopy := entry
		out = append(out, entryCopy)
	}
	return out
}

// withPathParams remains for older tests that still need direct net/http
// handlers. It is intentionally minimal.
type pathParamsKey struct{}

func withPathParams(r *http.Request, params map[string]string) *http.Request {
	if len(params) == 0 {
		return r
	}
	copyParams := make(map[string]string, len(params))
	for k, v := range params {
		copyParams[k] = v
	}
	return r.WithContext(context.WithValue(r.Context(), pathParamsKey{}, copyParams))
}
