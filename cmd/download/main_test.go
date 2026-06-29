package download

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestDownloadRunE_RequiresDID(t *testing.T) {
	downloadDID = "   "
	downloadOut = ""

	err := Cmd.RunE(&cobra.Command{}, nil)
	if err == nil || !strings.Contains(err.Error(), "--did is required") {
		t.Fatalf("expected missing did error, got: %v", err)
	}
}
