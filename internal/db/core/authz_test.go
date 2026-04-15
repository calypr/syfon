package core

import (
	"context"
	"testing"
)

func TestGetUserAuthz(t *testing.T) {
	t.Run("missing key returns empty", func(t *testing.T) {
		got := GetUserAuthz(context.Background())
		if len(got) != 0 {
			t.Fatalf("expected empty authz, got %v", got)
		}
	})

	t.Run("wrong type returns empty", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserAuthzKey, "not-a-list")
		got := GetUserAuthz(ctx)
		if len(got) != 0 {
			t.Fatalf("expected empty authz, got %v", got)
		}
	})

	t.Run("list returned as-is", func(t *testing.T) {
		expected := []string{"/programs/a", "/programs/a/projects/b"}
		ctx := context.WithValue(context.Background(), UserAuthzKey, expected)
		got := GetUserAuthz(ctx)
		if len(got) != len(expected) {
			t.Fatalf("expected %d resources, got %d", len(expected), len(got))
		}
		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("expected %q at index %d, got %q", expected[i], i, got[i])
			}
		}
	})
}

func TestCheckAccess(t *testing.T) {
	tests := []struct {
		name            string
		recordResources []string
		userResources   []string
		expected        bool
	}{
		{
			name:            "public record",
			recordResources: nil,
			userResources:   nil,
			expected:        true,
		},
		{
			name:            "single match",
			recordResources: []string{"/p/a"},
			userResources:   []string{"/p/a"},
			expected:        true,
		},
		{
			name:            "no match",
			recordResources: []string{"/p/a"},
			userResources:   []string{"/p/b"},
			expected:        false,
		},
		{
			name:            "any match",
			recordResources: []string{"/p/a", "/p/b"},
			userResources:   []string{"/p/c", "/p/b"},
			expected:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CheckAccess(tc.recordResources, tc.userResources)
			if got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestAuthHeaderAndMode(t *testing.T) {
	ctx := context.Background()
	if HasAuthHeader(ctx) {
		t.Fatalf("expected no auth header in empty context")
	}
	if IsGen3Mode(ctx) {
		t.Fatalf("expected not gen3 in empty context")
	}

	ctx = context.WithValue(ctx, AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, AuthModeKey, "gen3")
	if !HasAuthHeader(ctx) {
		t.Fatalf("expected auth header to be present")
	}
	if !IsGen3Mode(ctx) {
		t.Fatalf("expected gen3 mode")
	}
}

func TestGetUserPrivileges(t *testing.T) {
	t.Run("missing key returns empty map", func(t *testing.T) {
		got := GetUserPrivileges(context.Background())
		if len(got) != 0 {
			t.Fatalf("expected empty privileges, got %v", got)
		}
	})

	t.Run("wrong type returns empty map", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserPrivilegesKey, "bad")
		got := GetUserPrivileges(ctx)
		if len(got) != 0 {
			t.Fatalf("expected empty privileges, got %v", got)
		}
	})

	t.Run("valid map returned", func(t *testing.T) {
		expected := map[string]map[string]bool{
			"/programs/a/projects/b": {"read": true, "*": false},
		}
		ctx := context.WithValue(context.Background(), UserPrivilegesKey, expected)
		got := GetUserPrivileges(ctx)
		if !got["/programs/a/projects/b"]["read"] {
			t.Fatalf("expected read=true in privileges")
		}
	})
}

func TestHasMethodAccess(t *testing.T) {
	resource := "/programs/a/projects/b"

	t.Run("local mode allows", func(t *testing.T) {
		if !HasMethodAccess(context.Background(), "read", []string{resource}) {
			t.Fatalf("expected local mode to allow access")
		}
	})

	t.Run("gen3 without auth header denies", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), AuthModeKey, "gen3")
		if HasMethodAccess(ctx, "read", []string{resource}) {
			t.Fatalf("expected deny without auth header")
		}
	})

	t.Run("gen3 with empty resources denies", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, UserPrivilegesKey, map[string]map[string]bool{
			resource: {"read": true},
		})
		if HasMethodAccess(ctx, "read", nil) {
			t.Fatalf("expected deny for empty resource set")
		}
	})

	t.Run("gen3 allows method on all resources", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, UserPrivilegesKey, map[string]map[string]bool{
			resource:        {"read": true},
			"/programs/a/x": {"*": true},
		})
		if !HasMethodAccess(ctx, "read", []string{resource, "/programs/a/x"}) {
			t.Fatalf("expected allow with explicit and wildcard methods")
		}
	})

	t.Run("gen3 denies missing resource privilege", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, UserPrivilegesKey, map[string]map[string]bool{
			resource: {"read": true},
		})
		if HasMethodAccess(ctx, "read", []string{resource, "/programs/missing"}) {
			t.Fatalf("expected deny when any resource lacks privilege")
		}
	})

	t.Run("gen3 denies when method missing", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, UserPrivilegesKey, map[string]map[string]bool{
			resource: {"create": true},
		})
		if HasMethodAccess(ctx, "read", []string{resource}) {
			t.Fatalf("expected deny for missing method privilege")
		}
	})
}
