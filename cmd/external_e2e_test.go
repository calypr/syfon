package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSyfonExternalServerE2E runs CLI-only e2e checks against a live Syfon server.
//
// Enable with:
//
//	SYFON_E2E_SERVER_URL=http://10.96.14.83 go test ./cmd -run TestSyfonExternalServerE2E -v
//
// Optional:
//
//	SYFON_E2E_EXPECTED_HEALTH_PATH=/healthz
func TestSyfonExternalServerE2E(t *testing.T) {
	serverURL := strings.TrimSpace(os.Getenv("SYFON_E2E_SERVER_URL"))
	if serverURL == "" {
		t.Skip("set SYFON_E2E_SERVER_URL to run external e2e test")
	}

	// 1. Ping
	out, err := executeRootCommand(t, "--server", serverURL, "ping")
	if err != nil {
		t.Fatalf("ping failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "Syfon is reachable") {
		t.Fatalf("unexpected ping output: %s", out)
	}

	// 2. Upload
	srcPath := filepath.Join(t.TempDir(), "external-e2e-source.txt")
	srcData := []byte("syfon external e2e payload")
	if err := os.WriteFile(srcPath, srcData, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	out, err = executeRootCommand(t, "--server", serverURL, "upload", "--file", srcPath)
	if err != nil {
		t.Fatalf("upload failed: %v output=%s", err, out)
	}
	uploadedID, err := parseUploadedObjectID(out)
	if err != nil {
		t.Fatalf("parse upload output: %v output=%s", err, out)
	}

	// 3. Download + compare bytes
	outPath := filepath.Join(t.TempDir(), "external-e2e-downloaded.txt")
	out, err = executeRootCommand(t, "--server", serverURL, "download", "--did", uploadedID, "--out", outPath)
	if err != nil {
		t.Fatalf("download failed: %v output=%s", err, out)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(got, srcData) {
		t.Fatalf("downloaded bytes mismatch")
	}

	// 4. sha256sum updates record and prints the hash
	sumOut, err := executeRootCommand(t, "--server", serverURL, "sha256sum", "--did", uploadedID)
	if err != nil {
		t.Fatalf("sha256sum failed: %v output=%s", err, sumOut)
	}
	expectedHash := sha256.Sum256(srcData)
	expectedSum := hex.EncodeToString(expectedHash[:])
	if !strings.Contains(sumOut, expectedSum) {
		t.Fatalf("expected sha256 %s in output, got %s", expectedSum, sumOut)
	}
}

func parseUploadedObjectID(out string) (string, error) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "uploaded ") {
			parts := strings.Split(line, " ")
			if len(parts) > 0 {
				return parts[len(parts)-1], nil
			}
		}
	}
	return "", fmt.Errorf("missing uploaded line")
}
