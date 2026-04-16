package apitypes

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestNoDuplicateCoreAPITypesOutsideApigenAndAPITypes(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	pat := regexp.MustCompile(`\btype\s+(DRSObject|InternalRecord|OutputObject|AccessMethod|Checksum)\s+struct\b`)

	var violations []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == ".gocache" || name == ".gomodcache" || name == ".tmp" || name == "ga4gh" || name == "git-drs" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		norm := filepath.ToSlash(path)
		if strings.Contains(norm, "/apigen/") || strings.Contains(norm, "/api/types/") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if pat.Match(b) {
			violations = append(violations, norm)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("duplicate core API type declarations found outside apigen/api-types:\n%s", strings.Join(violations, "\n"))
	}
}
