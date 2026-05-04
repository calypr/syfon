package addurl

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestAddURLRunE_RequiresDID(t *testing.T) {
	addURLDid = "   "
	addURL = "s3://bucket/key"
	addURLOrg = "org"
	addURLProject = "project"

	err := Cmd.RunE(&cobra.Command{}, nil)
	if err == nil || !strings.Contains(err.Error(), "--did is required") {
		t.Fatalf("expected missing did error, got: %v", err)
	}
}

func TestAddURLRunE_RequiresURL(t *testing.T) {
	addURLDid = "did:123"
	addURL = "   "
	addURLOrg = "org"
	addURLProject = "project"

	err := Cmd.RunE(&cobra.Command{}, nil)
	if err == nil || !strings.Contains(err.Error(), "--url is required") {
		t.Fatalf("expected missing url error, got: %v", err)
	}
}

func TestAddURLRunE_RequiresOrg(t *testing.T) {
	addURLDid = "did:123"
	addURL = "s3://bucket/key"
	addURLOrg = "   "
	addURLProject = "project"

	err := Cmd.RunE(&cobra.Command{}, nil)
	if err == nil || !strings.Contains(err.Error(), "--org is required") {
		t.Fatalf("expected missing org error, got: %v", err)
	}
}

