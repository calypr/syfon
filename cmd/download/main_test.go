package download

import (
	"path/filepath"
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

func TestDefaultOutputFilenameUsesSafeBasename(t *testing.T) {
	name := "../nested/report.txt"
	got, err := safeDefaultOutputFilename(name)
	if err != nil {
		t.Fatalf("safeDefaultOutputFilename(%q) error = %v", name, err)
	}
	if got != filepath.Base(name) {
		t.Fatalf("safeDefaultOutputFilename(%q) = %q, want %q", name, got, filepath.Base(name))
	}
}

func TestDefaultOutputFilenameRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", "   ", ".", "..", string(filepath.Separator)} {
		t.Run(name, func(t *testing.T) {
			if got, err := safeDefaultOutputFilename(name); err == nil {
				t.Fatalf("safeDefaultOutputFilename(%q) = %q, want error", name, got)
			}
		})
	}
}

func TestDefaultOutputFilenameAcceptsOrdinaryAndAbsoluteNames(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "report.txt", want: "report.txt"},
		{name: filepath.Join(string(filepath.Separator), "tmp", "report.txt"), want: "report.txt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := safeDefaultOutputFilename(test.name)
			if err != nil {
				t.Fatalf("safeDefaultOutputFilename(%q) error = %v", test.name, err)
			}
			if got != test.want {
				t.Fatalf("safeDefaultOutputFilename(%q) = %q, want %q", test.name, got, test.want)
			}
		})
	}
}
