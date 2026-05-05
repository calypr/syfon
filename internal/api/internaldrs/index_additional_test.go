package internaldrs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
)

func TestParseInternalListPaginationFiber_InvalidInputs(t *testing.T) {
	om := core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{})

	cases := []struct {
		name string
		url  string
	}{
		{name: "invalid limit", url: "/index?limit=abc"},
		{name: "negative limit", url: "/index?limit=-1"},
		{name: "invalid page", url: "/index?page=abc"},
		{name: "negative page", url: "/index?page=-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			rr := doInternalDRSTestRequest(req, om)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d body=%s", tc.url, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHandleInternalBulkDocuments_InvalidBodyAndMissingIDs(t *testing.T) {
	om := core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{})

	req := httptest.NewRequest(http.MethodPost, "/bulk/documents", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rr := doInternalDRSTestRequestWithAlias(req, om, http.MethodPost, "/bulk/documents", handleInternalBulkDocumentsFiber(om))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid json, got %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/bulk/documents", strings.NewReader(`{"ids":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rr = doInternalDRSTestRequestWithAlias(req, om, http.MethodPost, "/bulk/documents", handleInternalBulkDocumentsFiber(om))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty ids, got %d body=%s", rr.Code, rr.Body.String())
	}
}

