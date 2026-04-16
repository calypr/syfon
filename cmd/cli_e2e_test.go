package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/internal/api/docs"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/api/metrics"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/provider"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/signer/file"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func executeRootCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	resetCommandFlags(RootCmd)
	var out bytes.Buffer
	var errOut bytes.Buffer
	RootCmd.SetOut(&out)
	RootCmd.SetErr(&errOut)
	RootCmd.SetArgs(args)
	err := RootCmd.Execute()
	return strings.TrimSpace(out.String() + errOut.String()), err
}

func resetCommandFlags(cmd *cobra.Command) {
	resetFlagSet(cmd.PersistentFlags())
	resetFlagSet(cmd.Flags())
	for _, child := range cmd.Commands() {
		resetCommandFlags(child)
	}
}

func resetFlagSet(fs *pflag.FlagSet) {
	fs.VisitAll(func(f *pflag.Flag) {
		_ = f.Value.Set(f.DefValue)
		f.Changed = false
	})
}

type fiberTestServer struct {
	URL string
	app *fiber.App
	ln  net.Listener
}

func (s *fiberTestServer) Close() {
	_ = s.app.Shutdown()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func newSyfonTestServer(t *testing.T) *fiberTestServer {
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
	fSigner, _ := file.NewFileSigner("/")
	uM.RegisterSigner(provider.File, fSigner)
	svc := service.NewObjectsAPIService(database, uM)

	app := fiber.New()
	app.Get(config.RouteHealthz, func(c fiber.Ctx) error {
		return c.SendString("OK")
	})
	api := app.Group("/")
	strict := service.NewStrictServer(svc)
	drs.RegisterHandlersWithOptions(api, drs.NewStrictHandler(strict, nil), drs.FiberServerOptions{
		BaseURL: "/ga4gh/drs/v1",
	})
	docs.RegisterSwaggerRoutes(app)
	metrics.RegisterMetricsRoutes(api, database)
	internaldrs.RegisterInternalIndexRoutes(api, database, uM)
	internaldrs.RegisterInternalDataRoutes(api, database, uM)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = app.Listener(ln)
	}()

	return &fiberTestServer{
		URL: "http://" + ln.Addr().String(),
		app: app,
		ln:  ln,
	}
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
	out, err := executeRootCommand(t, "--server", server.URL, "upload", "--file", srcPath, "--did", uploadDID, "--authz", "/programs/syfon/projects/e2e")
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

	c, err := syclient.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := c.Index().Get(context.Background(), uploadDID)
	if err != nil {
		t.Fatalf("fetch updated record: %v", err)
	}
	if rec.Hashes == nil {
		t.Fatalf("expected hashes in record, got nil")
	}
	if (*rec.Hashes)["sha256"] != expectedSum {
		t.Fatalf("expected sha256 in record: %s got: %s", expectedSum, (*rec.Hashes)["sha256"])
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
		"--authz", "/programs/syfon/projects/e2e",
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
