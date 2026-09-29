package usage

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/objects"
)

type hillclimbObjectStore struct {
	objects.ObjectStore
	record            drs.DrsObject
	requestedLoads    int
	policyLoads       int
	siblingIndexLoads int
}

func (s *hillclimbObjectStore) GetBulkObjects(_ context.Context, ids []string) ([]drs.DrsObject, error) {
	if reflect.DeepEqual(ids, []string{"shared", "missing"}) {
		s.requestedLoads++
		return []drs.DrsObject{s.record}, nil
	}
	return nil, fmt.Errorf("unexpected object IDs: %v", ids)
}

func (s *hillclimbObjectStore) GetPublicReadByIDs(_ context.Context, _ []string) (map[string]bool, error) {
	s.policyLoads++
	return nil, nil
}

func (s *hillclimbObjectStore) ListScopedObjectIDsByChecksums(_ context.Context, _, _ string, _ []string) (map[string][]string, error) {
	s.siblingIndexLoads++
	return nil, nil
}

func TestHillclimbAggregateUsageProfile(t *testing.T) {
	for _, count := range []int{1, 100} {
		for sample := 1; sample <= 3; sample++ {
			scopes := make([]Scope, count)
			resources := make([]string, count)
			privileges := make(map[string]map[string]bool, count)
			for i := range scopes {
				scopes[i] = Scope{Organization: "org", Project: fmt.Sprintf("p-%03d", i)}
				resources[i] = fmt.Sprintf("/organization/org/project/p-%03d", i)
				privileges[resources[i]] = map[string]bool{"read": true}
			}
			session := access.NewSession("local")
			session.AuthzEnforced = true
			session.SetAuthorizations(nil, privileges, true)
			ctx := access.WithSession(context.Background(), session)
			store := &hillclimbObjectStore{record: drs.DrsObject{Id: "shared", ControlledAccess: &resources}}
			service := NewService(Dependencies{Objects: objects.NewService(store)})
			ids, err := service.listReadableObjectIDs(ctx, ScopeQuery{Scopes: scopes}, []string{"shared", "missing"})
			if err != nil || !reflect.DeepEqual(ids, []string{"shared"}) {
				t.Fatalf("scopes=%d sample=%d ids=%v err=%v", count, sample, ids, err)
			}
			if store.requestedLoads < 1 || store.requestedLoads > count || store.policyLoads != count {
				t.Fatalf("scopes=%d sample=%d requested=%d policy=%d", count, sample, store.requestedLoads, store.policyLoads)
			}
			t.Logf("usage_profile scopes=%d sample=%d requested_loads=%d policy_loads=%d sibling_index_loads=%d", count, sample, store.requestedLoads, store.policyLoads, store.siblingIndexLoads)
		}
	}
}
