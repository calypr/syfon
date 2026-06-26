package services

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/repair"
)

func TestRepairServiceScopeAuditUsesScopeRoute(t *testing.T) {
	requester := &fakeRequester{responseJSON: []byte(`{"organization":"org","project":"proj","scanned":1}`)}
	service := NewRepairService(requester)

	report, err := service.ScopeAudit(context.Background(), ScopeRepairOptions{
		Organization: "org",
		ProjectID:    "proj",
		CheckStorage: true,
		Limit:        10,
		PageSize:     5,
	})
	if err != nil {
		t.Fatalf("ScopeAudit returned error: %v", err)
	}
	if requester.path != common.RouteInternalRepairScopeAudit {
		t.Fatalf("expected scope audit path %q, got %q", common.RouteInternalRepairScopeAudit, requester.path)
	}
	body, ok := requester.body.(repair.Options)
	if !ok {
		t.Fatalf("expected repair.Options body, got %T", requester.body)
	}
	if body.Organization != "org" || body.Project != "proj" || body.Limit != 10 || body.PageSize != 5 || !body.CheckStorage {
		t.Fatalf("unexpected scope audit body: %+v", body)
	}
	if report.Organization != "org" || report.Project != "proj" || report.Scanned != 1 {
		t.Fatalf("unexpected scope audit report: %+v", report)
	}
}

func TestRepairServiceScopeApplyUsesScopeRoute(t *testing.T) {
	requester := &fakeRequester{responseJSON: []byte(`{"report":{"organization":"org","project":"proj","scanned":1},"mutated":1,"auto_fixable":1}`)}
	service := NewRepairService(requester)

	result, err := service.ScopeApply(context.Background(), ScopeRepairOptions{
		Organization: "org",
		ProjectID:    "proj",
	})
	if err != nil {
		t.Fatalf("ScopeApply returned error: %v", err)
	}
	if requester.path != common.RouteInternalRepairScopeApply {
		t.Fatalf("expected scope apply path %q, got %q", common.RouteInternalRepairScopeApply, requester.path)
	}
	body, ok := requester.body.(repair.Options)
	if !ok {
		t.Fatalf("expected repair.Options body, got %T", requester.body)
	}
	if body.Organization != "org" || body.Project != "proj" {
		t.Fatalf("unexpected scope apply body: %+v", body)
	}
	if result.Mutated != 1 || result.AutoFixable != 1 || result.Report.Organization != "org" {
		t.Fatalf("unexpected scope apply result: %+v", result)
	}
}
