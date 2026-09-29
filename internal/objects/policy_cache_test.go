package objects_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/objects"
)

type policyCountingStore struct {
	*objectTestStore
	flags      map[string]bool
	calls      [][]string
	failOnCall int
	failErr    error
	afterCall  func()
}

func (s *policyCountingStore) GetPublicReadByIDs(_ context.Context, ids []string) (map[string]bool, error) {
	s.calls = append(s.calls, append([]string(nil), ids...))
	if s.failOnCall == len(s.calls) {
		return nil, s.failErr
	}
	result := make(map[string]bool)
	for _, id := range ids {
		if value, ok := s.flags[id]; ok {
			result[id] = value
		}
	}
	if s.afterCall != nil {
		s.afterCall()
	}
	return result, nil
}

func TestReadableScopesReuseMissingPolicyRowsAndRefreshNextOperation(t *testing.T) {
	first := "/organization/org/project/first"
	second := "/organization/org/project/second"
	resources := []string{first, second}
	store := &policyCountingStore{objectTestStore: &objectTestStore{Objects: map[string]*drs.DrsObject{
		"shared": {Id: "shared", ControlledAccess: &resources},
	}}, flags: map[string]bool{}}
	service := objects.NewService(store)
	scopes := []objects.Scope{{Organization: "org", Project: "first"}, {Organization: "org", Project: "second"}}
	ctx := buildLocalAuthzContext(nil)
	for run := 1; run <= 2; run++ {
		ids, err := service.ListReadableObjectIDsAmongScopes(ctx, scopes, []string{"shared"})
		if err != nil || len(ids) != 0 {
			t.Fatalf("run=%d unreadable IDs=%v error=%v", run, ids, err)
		}
		if len(store.calls) != run || !slices.Equal(store.calls[run-1], []string{"shared"}) {
			t.Fatalf("run=%d policy calls=%v", run, store.calls)
		}
	}
	store.flags["shared"] = true
	ids, err := service.ListReadableObjectIDsAmongScopes(ctx, scopes, []string{"shared"})
	if err != nil || !slices.Equal(ids, []string{"shared"}) || len(store.calls) != 3 {
		t.Fatalf("refreshed public ID=%v error=%v calls=%v", ids, err, store.calls)
	}
}

func TestReadableScopesKeepPublicPrivateAndForbiddenPolicies(t *testing.T) {
	public := "/organization/org/project/public"
	private := "/organization/org/project/private"
	forbidden := "/organization/org/project/forbidden"
	store := &policyCountingStore{objectTestStore: &objectTestStore{Objects: map[string]*drs.DrsObject{
		"public":    {Id: "public", ControlledAccess: &[]string{public}},
		"private":   {Id: "private", ControlledAccess: &[]string{private}},
		"forbidden": {Id: "forbidden", ControlledAccess: &[]string{forbidden}},
	}}, flags: map[string]bool{"public": true, "private": false}}
	ctx := buildLocalAuthzContext(map[string]map[string]bool{private: {"read": true}})
	scopes := []objects.Scope{{Organization: "org", Project: "public"}, {Organization: "org", Project: "private"}, {Organization: "org", Project: "forbidden"}}
	ids, err := objects.NewService(store).ListReadableObjectIDsAmongScopes(ctx, scopes, []string{"public", "private", "forbidden"})
	if err != nil || !slices.Equal(ids, []string{"public", "private"}) {
		t.Fatalf("readable IDs=%v error=%v", ids, err)
	}
	if len(store.calls) != 1 || !slices.Equal(store.calls[0], []string{"public", "private", "forbidden"}) {
		t.Fatalf("policy batches=%v", store.calls)
	}
}

func TestReadableScopesFetchNewSiblingPolicyOnce(t *testing.T) {
	resource := "/organization/org/project/project"
	checksum := strings.Repeat("a", 64)
	store := &policyCountingStore{objectTestStore: &objectTestStore{Objects: map[string]*drs.DrsObject{
		"later":   {Id: "later", CreatedTime: drsISOTime("2026-01-02T00:00:00Z"), ControlledAccess: &[]string{resource}, Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}}},
		"earlier": {Id: "earlier", CreatedTime: drsISOTime("2026-01-01T00:00:00Z"), ControlledAccess: &[]string{resource}, Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}}},
	}}, flags: map[string]bool{"earlier": true}}
	scopes := []objects.Scope{{Organization: "org", Project: "project"}, {Organization: "org", Project: "project"}}
	ids, err := objects.NewService(store).ListReadableObjectIDsAmongScopes(buildLocalAuthzContext(nil), scopes, []string{"later"})
	if err != nil || !slices.Equal(ids, []string{"earlier"}) {
		t.Fatalf("sibling readable IDs=%v error=%v", ids, err)
	}
	if !reflect.DeepEqual(store.calls, [][]string{{"later"}, {"earlier"}}) {
		t.Fatalf("policy batches=%v, want requested then sibling once", store.calls)
	}
}

func TestReadableScopesPropagatePolicyFailuresAndWarmCacheCancellation(t *testing.T) {
	resource := "/organization/org/project/project"
	checksum := strings.Repeat("b", 64)
	newStore := func() *policyCountingStore {
		return &policyCountingStore{objectTestStore: &objectTestStore{Objects: map[string]*drs.DrsObject{
			"later":   {Id: "later", ControlledAccess: &[]string{resource}, Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}}},
			"earlier": {Id: "earlier", ControlledAccess: &[]string{resource}, Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}}},
		}}}
	}
	scope := []objects.Scope{{Organization: "org", Project: "project"}}
	policyErr := errors.New("public policy unavailable")
	for _, failOn := range []int{1, 2} {
		store := newStore()
		store.failOnCall, store.failErr = failOn, policyErr
		_, err := objects.NewService(store).ListReadableObjectIDsAmongScopes(buildLocalAuthzContext(nil), scope, []string{"later"})
		if !errors.Is(err, policyErr) || len(store.calls) != failOn {
			t.Fatalf("failure on call=%d error=%v calls=%v", failOn, err, store.calls)
		}
	}
	store := newStore()
	ctx, cancel := context.WithCancel(buildLocalAuthzContext(nil))
	store.afterCall = cancel
	_, err := objects.NewService(store).ListReadableObjectIDsAmongScopes(ctx, scope, []string{"later"})
	if !errors.Is(err, context.Canceled) || len(store.calls) != 1 {
		t.Fatalf("warm-cache cancellation error=%v calls=%v", err, store.calls)
	}
}
