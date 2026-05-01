package common

import (
	"reflect"
	"sort"
	"testing"
)

func TestResourceAndAuthzHelpers(t *testing.T) {
	t.Run("resource path", func(t *testing.T) {
		tests := []struct {
			name    string
			org     string
			project string
			want    string
			wantErr bool
		}{
			{name: "empty", want: ""},
			{name: "org only", org: "syfon", want: "/programs/syfon"},
			{name: "org and project", org: "syfon", project: "e2e", want: "/programs/syfon/projects/e2e"},
			{name: "project without org", project: "e2e", wantErr: true},
		}

		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got, err := ResourcePath(tc.org, tc.project)
				if tc.wantErr {
					if err == nil {
						t.Fatalf("expected error, got path=%q", got)
					}
					return
				}
				if err != nil {
					t.Fatalf("ResourcePath returned error: %v", err)
				}
				if got != tc.want {
					t.Fatalf("ResourcePath(%q,%q) = %q, want %q", tc.org, tc.project, got, tc.want)
				}
			})
		}
	})

	t.Run("storage prefix", func(t *testing.T) {
		if got := StoragePrefix("", ""); got != "" {
			t.Fatalf("expected empty prefix, got %q", got)
		}
		if got := StoragePrefix("syfon", ""); got != "" {
			t.Fatalf("unexpected org prefix: %q", got)
		}
		if got := StoragePrefix("syfon", "e2e"); got != "" {
			t.Fatalf("unexpected project prefix: %q", got)
		}
	})

	t.Run("checksum normalization", func(t *testing.T) {
		if got := NormalizeChecksum("  sha256:ABC123  "); got != "ABC123" {
			t.Fatalf("unexpected normalized checksum: %q", got)
		}
	})

	t.Run("authz map from scope", func(t *testing.T) {
		if got := AuthzMapFromScope("", ""); got != nil {
			t.Fatalf("expected nil authz map, got %+v", got)
		}
		if got := AuthzMapFromScope("syfon", ""); !reflect.DeepEqual(got, map[string][]string{"syfon": []string{}}) {
			t.Fatalf("unexpected org-wide authz map: %+v", got)
		}
		if got := AuthzMapFromScope("syfon", "e2e"); !reflect.DeepEqual(got, map[string][]string{"syfon": []string{"e2e"}}) {
			t.Fatalf("unexpected project authz map: %+v", got)
		}
	})

	t.Run("authz list to map", func(t *testing.T) {
		got := AuthzListToMap([]string{
			"/programs/syfon",
			" /programs/syfon/projects/e2e ",
			"/programs/other/projects/proj-1",
		})
		want := map[string][]string{
			"syfon": []string{"e2e"},
			"other": []string{"proj-1"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("AuthzListToMap mismatch:\n got: %+v\nwant: %+v", got, want)
		}
	})

	t.Run("authz map to list", func(t *testing.T) {
		got := AuthzMapToList(map[string][]string{
			"syfon": {"e2e", "e2e-2"},
			"other": {},
		})
		sort.Strings(got)
		want := []string{
			"/programs/other",
			"/programs/syfon/projects/e2e",
			"/programs/syfon/projects/e2e-2",
		}
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("AuthzMapToList mismatch:\n got: %+v\nwant: %+v", got, want)
		}
	})

	t.Run("authz map matches scope", func(t *testing.T) {
		projectScoped := map[string][]string{"syfon": {"e2e"}}
		if !AuthzMapMatchesScope(projectScoped, "syfon", "e2e") {
			t.Fatal("expected project-scoped match")
		}
		if AuthzMapMatchesScope(projectScoped, "syfon", "other") {
			t.Fatal("expected project-scoped miss")
		}

		orgWide := map[string][]string{"syfon": {}}
		if !AuthzMapMatchesScope(orgWide, "syfon", "anything") {
			t.Fatal("expected org-wide match")
		}
		if AuthzMapMatchesScope(nil, "syfon", "e2e") {
			t.Fatal("expected nil map miss")
		}
	})
}
