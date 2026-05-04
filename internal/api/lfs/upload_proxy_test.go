package lfs

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestLFSUploadProxyNoBucket507(t *testing.T) {
	ResetLFSLimitersForTest()
	router, db := newLFSRouter()
	db.Credentials = map[string]models.S3Credential{}
	db.NoDefaultCreds = true
	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", bytes.NewReader([]byte("x")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("expected 507, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSUploadProxySuccess(t *testing.T) {
	// 1. Setup mock upload server
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", "mock-etag")
		w.WriteHeader(http.StatusOK)
	}))
	defer uploadServer.Close()

	// 2. Setup Syfon router with mocked dependencies
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	uM := &customMockUrlManager{uploadURL: uploadServer.URL}
	app := fiber.New()
	om := core.NewObjectManager(db, uM)
	RegisterLFSRoutes(app, om, DefaultOptions())
	router := &fiberTestRouter{app: app}

	// 3. Perform upload proxy request
	oid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader([]byte("small content")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// 4. Verify results
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if uM.initCalled != 1 {
		t.Errorf("expected 1 InitMultipartUpload call, got %d", uM.initCalled)
	}
	if uM.completeCalled != 1 {
		t.Errorf("expected 1 CompleteMultipartUpload call, got %d", uM.completeCalled)
	}
}
