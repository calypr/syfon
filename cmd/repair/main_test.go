package repair

import (
	"testing"

	repairsvc "github.com/calypr/syfon/internal/repair"
	"github.com/spf13/cobra"
)

func TestApplyRequiresScope(t *testing.T) {
	cmd := newCommand(
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupAuditRequest) error { return nil },
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupApplyRequest) error { return nil },
	)
	cmd.SetArgs([]string{"apply", "--delete-stale-duplicates"})
	err := cmd.Execute()
	if err == nil || err.Error() != "repair apply requires --organization and --project" {
		t.Fatalf("expected scope validation error, got %v", err)
	}
}

func TestAuditCollectsCheckStorageFlag(t *testing.T) {
	called := false
	cmd := newCommand(
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupAuditRequest) error {
			called = true
			if opts.CheckStorage == nil || !*opts.CheckStorage {
				t.Fatal("expected check-storage true")
			}
			return nil
		},
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupApplyRequest) error { return nil },
	)
	cmd.SetArgs([]string{"audit", "--check-storage"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("audit failed: %v", err)
	}
	if !called {
		t.Fatal("expected audit runner call")
	}
}

func TestApplyRequiresDeleteMode(t *testing.T) {
	cmd := newCommand(
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupAuditRequest) error { return nil },
		func(cmd *cobra.Command, opts repairsvc.StorageCleanupApplyRequest) error { return nil },
	)
	cmd.SetArgs([]string{"apply", "--organization", "org", "--project", "proj"})
	err := cmd.Execute()
	if err == nil || err.Error() != "repair apply requires --delete-stale-duplicates and/or --delete-repo-orphans" {
		t.Fatalf("expected delete mode validation error, got %v", err)
	}
}
