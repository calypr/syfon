package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/api/coreapi"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/api/metrics"
	"github.com/calypr/syfon/service"
	"github.com/calypr/syfon/urlmanager"
	"github.com/google/uuid"
)

func executeRootCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	var errOut bytes.Buffer
	RootCmd.SetOut(&out)
	RootCmd.SetErr(&errOut)
	RootCmd.SetArgs(args)
	err := RootCmd.Execute()
	return strings.TrimSpace(out.String() + errOut.String()), err
}

func newSyfonTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	storageDir := t.TempDir()
	endpoint := "file://" + storageDir

	database := db.NewInMemoryDB()
	if err := database.SaveS3Credential(context.Background(), &core.S3Credential{
		Bucket:   "syfon-bucket",
		Provider: "file",
		Endpoint: endpoint,
	}); err != nil {
		t.Fatalf("save test credential: %v", err)
	}

	uM := urlmanager.NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})
	svc := service.NewObjectsAPIService(database, uM)

	objectsController := drs.NewObjectsAPIController(svc)
	serviceInfoController := drs.NewServiceInfoAPIController(svc)
	uploadController := drs.NewUploadRequestAPIController(svc)

	router := drs.NewRouter(objectsController, serviceInfoController, uploadController)
	router.HandleFunc(config.RouteHealthz, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	coreapi.RegisterCoreRoutes(router, database)
	metrics.RegisterMetricsRoutes(router, database)
	internaldrs.RegisterInternalIndexRoutes(router, database)
	internaldrs.RegisterInternalDataRoutes(router, database, uM)

	return httptest.NewServer(router)
}

func TestSyfonVersionAndPing(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	out, err := executeRootCommand(t, "--server", server.URL, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}
	if !strings.Contains(out, "Syfon ") {
		t.Fatalf("unexpected version output: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "ping")
	if err != nil {
		t.Fatalf("ping command failed: %v", err)
	}
	if !strings.Contains(out, "Syfon is reachable") {
		t.Fatalf("unexpected ping output: %s", out)
	}
}

func TestSyfonUploadDownloadAddURLAndSHA256(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	srcPath := filepath.Join(t.TempDir(), "source.txt")
	srcData := []byte("syfon e2e upload payload")
	if err := os.WriteFile(srcPath, srcData, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	uploadDID := uuid.NewString()
	out, err := executeRootCommand(t, "--server", server.URL, "upload", "--file", srcPath, "--did", uploadDID)
	if err != nil {
		t.Fatalf("upload command failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "uploaded") {
		t.Fatalf("unexpected upload output: %s", out)
	}

	downloadPath := filepath.Join(t.TempDir(), "downloaded.txt")
	out, err = executeRootCommand(t, "--server", server.URL, "download", "--did", uploadDID, "--out", downloadPath)
	if err != nil {
		t.Fatalf("download command failed: %v output=%s", err, out)
	}
	downloadedData, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(downloadedData, srcData) {
		t.Fatalf("downloaded bytes mismatch")
	}

	hashOut, err := executeRootCommand(t, "--server", server.URL, "sha256sum", "--did", uploadDID)
	if err != nil {
		t.Fatalf("sha256sum command failed: %v output=%s", err, hashOut)
	}
	expectedHash := sha256.Sum256(srcData)
	expectedSum := hex.EncodeToString(expectedHash[:])
	if !strings.Contains(hashOut, expectedSum) {
		t.Fatalf("sha256sum output missing expected hash: %s", hashOut)
	}

	serverBaseURL = server.URL
	rec, err := getInternalRecord(uploadDID)
	if err != nil {
		t.Fatalf("fetch updated record: %v", err)
	}
	if rec.GetHashes()["sha256"] != expectedSum {
		t.Fatalf("expected sha256 in record: %s got: %s", expectedSum, rec.GetHashes()["sha256"])
	}

	externalSource := filepath.Join(t.TempDir(), "existing-url-source.txt")
	externalData := []byte("syfon add-url payload")
	if err := os.WriteFile(externalSource, externalData, 0o644); err != nil {
		t.Fatalf("write external source file: %v", err)
	}
	fileURL := "file://" + externalSource
	addURLDID := uuid.NewString()
	out, err = executeRootCommand(
		t,
		"--server", server.URL,
		"add-url",
		"--did", addURLDID,
		"--url", fileURL,
		"--name", "existing-url-source.txt",
		"--size", "21",
	)
	if err != nil {
		t.Fatalf("add-url command failed: %v output=%s", err, out)
	}

	downloadPath2 := filepath.Join(t.TempDir(), "downloaded-addurl.txt")
	out, err = executeRootCommand(t, "--server", server.URL, "download", "--did", addURLDID, "--out", downloadPath2)
	if err != nil {
		t.Fatalf("download(add-url) command failed: %v output=%s", err, out)
	}
	got2, err := os.ReadFile(downloadPath2)
	if err != nil {
		t.Fatalf("read downloaded add-url file: %v", err)
	}
	if !bytes.Equal(got2, externalData) {
		t.Fatalf("download(add-url) bytes mismatch")
	}
}
