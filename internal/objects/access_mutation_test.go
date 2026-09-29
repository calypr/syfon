package objects_test

import (
	"errors"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/objects"
)

func TestRegistrationAccessPreservesExistingContentBoundaries(t *testing.T) {
	const source = "/organization/org/project/source"
	const target = "/organization/org/project/target"
	private := &objects.ContentAccess{Resources: []string{source}}
	for _, tc := range []struct {
		name       string
		current    *objects.ContentAccess
		incoming   []string
		methods    *[]drs.AccessMethod
		privileges map[string]map[string]bool
		allowed    bool
	}{
		{"new content needs create", nil, []string{target}, nil, nil, false},
		{"new content with create", nil, []string{target}, nil, map[string]map[string]bool{target: {"create": true}}, true},
		{"private copy needs source read", private, []string{target}, nil, map[string]map[string]bool{target: {"create": true}}, false},
		{"private copy needs target create", private, []string{target}, nil, map[string]map[string]bool{source: {"read": true}}, false},
		{"authorized private copy", private, []string{target}, nil, map[string]map[string]bool{source: {"read": true}, target: {"create": true}}, true},
		{"public copy needs only target create", &objects.ContentAccess{Resources: []string{source}, PublicRead: true}, []string{target}, nil, map[string]map[string]bool{target: {"create": true}}, true},
		{"existing resource adds no grant", private, []string{source}, nil, nil, true},
		{"explicit access methods require read", private, []string{source}, &[]drs.AccessMethod{}, nil, false},
		{"private orphan cannot be claimed", &objects.ContentAccess{}, []string{target}, nil, map[string]map[string]bool{target: {"create": true}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			incoming := drs.DrsObject{ControlledAccess: &tc.incoming, AccessMethods: tc.methods}
			err := objects.AuthorizeRegistration(buildLocalAuthzContext(tc.privileges), &incoming, tc.current)
			if tc.allowed && err != nil {
				t.Fatalf("registration denied: %v", err)
			}
			if !tc.allowed && !errors.Is(err, errorapi.ErrAccessDenied) {
				t.Fatalf("registration error = %v, want access denied", err)
			}
		})
	}
}

func TestReplacementResourcePolicyChecksOnlyAddedGrants(t *testing.T) {
	const source = "/organization/org/project/source"
	const target = "/organization/org/project/target"
	current := objects.ContentAccess{Resources: []string{source}}
	ctx := buildLocalAuthzContext(map[string]map[string]bool{source: {"update": true}})
	for _, resources := range [][]string{nil, {source}} {
		if err := objects.AuthorizeReplacementResources(ctx, resources, current); err != nil {
			t.Fatalf("replacement without added grants: %v", err)
		}
	}
	if err := objects.AuthorizeReplacementResources(ctx, []string{source, target}, current); !errors.Is(err, errorapi.ErrAccessDenied) {
		t.Fatalf("replacement added an unauthorized grant: %v", err)
	}
	ctx = buildLocalAuthzContext(map[string]map[string]bool{source: {"read": true, "update": true}, target: {"create": true}})
	if err := objects.AuthorizeReplacementResources(ctx, []string{source, target}, current); err != nil {
		t.Fatalf("authorized replacement: %v", err)
	}
}
