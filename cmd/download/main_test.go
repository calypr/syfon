package download

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	syclient "github.com/calypr/syfon/client"
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

func TestDownloadURLToPath_FileScheme(t *testing.T) {
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "source.txt")
	outPath := filepath.Join(srcDir, "out.txt")
	payload := []byte("download-file-scheme")
	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	err := downloadURLToPath(context.Background(), "file://"+srcPath, outPath, nil)
	if err != nil {
		t.Fatalf("downloadURLToPath(file) error: %v", err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected output content: got=%q want=%q", string(got), string(payload))
	}
}

func TestDownloadURLToPath_HTTP_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("http-payload"))
	}))
	defer ts.Close()

	client, err := syclient.New(ts.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "out.txt")

	err = downloadURLToPath(context.Background(), ts.URL+"/download", outPath, client)
	if err != nil {
		t.Fatalf("downloadURLToPath(http success) error: %v", err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "http-payload" {
		t.Fatalf("unexpected output content: got=%q", string(got))
	}
}

func TestDownloadURLToPath_HTTP_ErrorStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadRequest)
	}))
	defer ts.Close()

	client, err := syclient.New(ts.URL)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "out.txt")

	err = downloadURLToPath(context.Background(), ts.URL+"/download", outPath, client)
	if err == nil || !strings.Contains(err.Error(), "download failed status=400") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestDownloadURLToPath_UnsupportedScheme(t *testing.T) {
	err := downloadURLToPath(context.Background(), "s3://bucket/key", filepath.Join(t.TempDir(), "out.txt"), nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported download url scheme") {
		t.Fatalf("expected unsupported scheme error, got: %v", err)
	}
}

func TestDownloadURLToPath_HTTP_NonConcreteClient(t *testing.T) {
	err := downloadURLToPath(context.Background(), "https://example.test/file", filepath.Join(t.TempDir(), "out.txt"), nil)
	if err == nil || !strings.Contains(err.Error(), "client implementation does not support raw requests") {
		t.Fatalf("expected non-concrete client error, got: %v", err)
	}
}

