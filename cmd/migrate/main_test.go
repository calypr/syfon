package migratecmd

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestNormalizedDumpPathDefaultsExport(t *testing.T) {
	if got := normalizedDumpPath(""); got != "indexd-records.sqlite" {
		t.Fatalf("expected default dump path, got %q", got)
	}
	if got := normalizedDumpPath(" custom.sqlite "); got != "custom.sqlite" {
		t.Fatalf("expected trimmed custom dump path, got %q", got)
	}
}

func TestIndexdURLFromServerUsesRootServerFlag(t *testing.T) {
	root := &cobra.Command{Use: "syfon"}
	root.PersistentFlags().String("server", "https://calypr-dev.ohsu.edu/", "")
	cmd := &cobra.Command{Use: "export"}
	root.AddCommand(cmd)

	got, err := indexdURLFromServer(cmd)
	if err != nil {
		t.Fatalf("indexdURLFromServer returned error: %v", err)
	}
	if got != "https://calypr-dev.ohsu.edu/index/index" {
		t.Fatalf("unexpected indexd url: %q", got)
	}
}

func TestTargetAuthFromInputsSupportsBasicAuth(t *testing.T) {
	auth, err := targetAuthFromInputs("", "", "drs-user", "drs-pass")
	if err != nil {
		t.Fatalf("targetAuthFromInputs returned error: %v", err)
	}
	if auth.Basic == nil || auth.Basic.Username != "drs-user" || auth.Basic.Password != "drs-pass" {
		t.Fatalf("unexpected basic auth config: %+v", auth.Basic)
	}
}

func TestTargetAuthFromInputsRequiresBasicPair(t *testing.T) {
	if _, err := targetAuthFromInputs("", "", "drs-user", ""); err == nil {
		t.Fatal("expected missing password error")
	}
	if _, err := targetAuthFromInputs("", "", "", "drs-pass"); err == nil {
		t.Fatal("expected missing username error")
	}
}

func TestTargetAuthFromInputsRejectsMixedAuth(t *testing.T) {
	if _, err := targetAuthFromInputs("", "token", "drs-user", "drs-pass"); err == nil {
		t.Fatal("expected token/basic conflict error")
	}
	if _, err := targetAuthFromInputs("profile", "", "drs-user", "drs-pass"); err == nil {
		t.Fatal("expected profile/basic conflict error")
	}
}
