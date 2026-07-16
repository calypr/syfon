package internaldrs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
)

func TestScopeRepairApplyRejectsReadOnlyProjectUser(t *testing.T) {
	resource, err := sycommon.ResourcePath("org", "proj")
	if err != nil {
		t.Fatalf("resource path: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/data/repair/project-scope/apply", strings.NewReader(`{"organization":"org","project":"proj"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(policyTestContext("gen3", true, map[string]map[string]bool{
		resource: {"read": true},
	}))

	rr := doInternalDRSTestRequest(req, core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only caller, got %d body=%s", rr.Code, rr.Body.String())
	}
}
