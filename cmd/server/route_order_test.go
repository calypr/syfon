package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/gorilla/mux"
)

type testRouter struct {
	routes []drs.Route
}

func (t *testRouter) Routes() drs.Routes {
	out := make(drs.Routes, len(t.routes))
	for _, r := range t.routes {
		out[r.Name] = r
	}
	return out
}

func (t *testRouter) OrderedRoutes() []drs.Route {
	return t.routes
}

func TestRegisterControllerRoutes_StaticBeforeParam(t *testing.T) {
	r := mux.NewRouter()
	api := &testRouter{
		routes: []drs.Route{
			{
				Name:    "PostObject",
				Method:  http.MethodPost,
				Pattern: "/ga4gh/drs/v1/objects/{object_id}",
				HandlerFunc: func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusTeapot)
				},
			},
			{
				Name:    "RegisterObjects",
				Method:  http.MethodPost,
				Pattern: "/ga4gh/drs/v1/objects/register",
				HandlerFunc: func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusCreated)
				},
			},
		},
	}

	registerControllerRoutes(r, api)

	req := httptest.NewRequest(http.MethodPost, "/ga4gh/drs/v1/objects/register", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected /objects/register to match static route first, got status %d", rr.Code)
	}
}

func TestRegisterAPIRoutes_StaticBeforeParamAcrossControllers(t *testing.T) {
	r := mux.NewRouter()
	objectsAPI := &testRouter{
		routes: []drs.Route{
			{
				Name:    "PostObject",
				Method:  http.MethodPost,
				Pattern: "/ga4gh/drs/v1/objects/{object_id}",
				HandlerFunc: func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusTeapot)
				},
			},
		},
	}
	registerAPI := &testRouter{
		routes: []drs.Route{
			{
				Name:    "RegisterObjects",
				Method:  http.MethodPost,
				Pattern: "/ga4gh/drs/v1/objects/register",
				HandlerFunc: func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusCreated)
				},
			},
		},
	}

	registerAPIRoutes(r, objectsAPI, registerAPI)

	req := httptest.NewRequest(http.MethodPost, "/ga4gh/drs/v1/objects/register", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected /objects/register to match static route first across controllers, got status %d", rr.Code)
	}
}
