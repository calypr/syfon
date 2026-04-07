package internaldrs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
)

func TestHandleMigrateBulk_RequiresCreatePrivilegeInGen3(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	body := `{"records":[{"id":"obj-1","size":1,"checksums":[{"type":"sha256","checksum":"abc"}],"authz":["/programs/p/projects/a"],"created_time":"2024-01-01T00:00:00Z"}]}`
	req := httptest.NewRequest(http.MethodPost, "/index/migrate/bulk", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/a": {"read": true},
	})
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handleMigrateBulk(mockDB).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(mockDB.Objects) != 0 {
		t.Fatalf("expected no objects written, got %d", len(mockDB.Objects))
	}
}

func TestHandleMigrateBulk_MissingAuthHeaderDeniedInGen3(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	body := `{"records":[{"id":"obj-2","size":1,"checksums":[{"type":"sha256","checksum":"abc"}],"created_time":"2024-01-01T00:00:00Z"}]}`
	req := httptest.NewRequest(http.MethodPost, "/index/migrate/bulk", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"create": true},
	})
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handleMigrateBulk(mockDB).ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleMigrateBulk_AllowsCreateAndDefaultsAuthz(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	body := `{"records":[{"id":"obj-3","name":"f","size":1,"version":"v1","description":"d","checksums":[{"type":"sha256","checksum":"abc"}],"created_time":"2024-01-01T00:00:00Z","updated_time":"2024-01-02T00:00:00Z"}]}`
	req := httptest.NewRequest(http.MethodPost, "/index/migrate/bulk", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"create": true},
	})
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handleMigrateBulk(mockDB).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	obj, ok := mockDB.Objects["obj-3"]
	if !ok {
		t.Fatal("expected object obj-3 to be written")
	}
	if obj.Version != "v1" || obj.Description != "d" {
		t.Fatalf("expected metadata preserved, got version=%q description=%q", obj.Version, obj.Description)
	}
	if got := mockDB.ObjectAuthz["obj-3"]; len(got) != 1 || got[0] != "/data_file" {
		t.Fatalf("expected default authz [/data_file], got %v", got)
	}
	if obj.CreatedTime.IsZero() || obj.UpdatedTime.IsZero() {
		t.Fatal("expected timestamps set")
	}
	if obj.CreatedTime.Format(time.RFC3339) != "2024-01-01T00:00:00Z" {
		t.Fatalf("unexpected created_time: %s", obj.CreatedTime.Format(time.RFC3339))
	}
}

