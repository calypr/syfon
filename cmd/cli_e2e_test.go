package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/internal/api/docs"
	"github.com/calypr/syfon/internal/api/drsapi"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/api/metrics"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
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

func TestSyfonMetricsTransfersCLI(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	now := time.Now().UTC()
	if err := server.DB.RecordTransferAttributionEvents(context.Background(), []models.TransferAttributionEvent{
		{
			EventID:        "cli-grant-1",
			AccessGrantID:  "cli-grant-1",
			EventType:      models.TransferEventAccessIssued,
			Direction:      models.ProviderTransferDirectionDownload,
			EventTime:      now.Add(-time.Minute),
			ObjectID:       "did-cli-1",
			SHA256:         "sha-cli-1",
			ObjectSize:     123,
			Organization:   "syfon",
			Project:        "e2e",
			AccessID:       "s3",
			Provider:       "file",
			Bucket:         "syfon-bucket",
			StorageURL:     "file://syfon-bucket/sha-cli-1",
			BytesRequested: 123,
			ActorEmail:     "user@example.com",
			ActorSubject:   "user@example.com",
		},
		{
			EventID:        "cli-grant-2",
			AccessGrantID:  "cli-grant-2",
			EventType:      models.TransferEventAccessIssued,
			Direction:      models.ProviderTransferDirectionUpload,
			EventTime:      now.Add(-30 * time.Second),
			ObjectID:       "did-cli-2",
			SHA256:         "sha-cli-2",
			ObjectSize:     50,
			Organization:   "syfon",
			Project:        "e2e",
			AccessID:       "s3",
			Provider:       "file",
			Bucket:         "syfon-bucket",
			StorageURL:     "file://syfon-bucket/sha-cli-2",
			BytesRequested: 50,
			ActorEmail:     "user@example.com",
			ActorSubject:   "user@example.com",
		},
	}); err != nil {
		t.Fatalf("record access grant: %v", err)
	}
	if err := server.DB.RecordProviderTransferEvents(context.Background(), []models.ProviderTransferEvent{
		{
			ProviderEventID:      "cli-transfer-1",
			AccessGrantID:        "cli-grant-1",
			Direction:            models.ProviderTransferDirectionDownload,
			EventTime:            now,
			ObjectID:             "did-cli-1",
			SHA256:               "sha-cli-1",
			Organization:         "syfon",
			Project:              "e2e",
			Provider:             "file",
			Bucket:               "syfon-bucket",
			StorageURL:           "file://syfon-bucket/sha-cli-1",
			BytesTransferred:     123,
			ActorEmail:           "user@example.com",
			ActorSubject:         "user@example.com",
			ReconciliationStatus: models.ProviderTransferMatched,
		},
	}); err != nil {
		t.Fatalf("record transfer event: %v", err)
	}
	syncFrom := now.Add(-2 * time.Minute).Format(time.RFC3339)
	syncTo := now.Add(2 * time.Minute).Format(time.RFC3339)

	out, err := executeRootCommand(t, "--server", server.URL, "metrics", "transfers", "summary", "--organization", "syfon", "--project", "e2e", "--user", "user@example.com", "--provider", "file", "--bucket", "syfon-bucket", "--from", syncFrom, "--to", syncTo)
	if err != nil {
		t.Fatalf("metrics transfers summary command failed: %v output=%s", err, out)
	}
	var summary models.TransferAttributionSummary
	if err := json.Unmarshal([]byte(out), &summary); err != nil {
		t.Fatalf("decode summary output %q: %v", out, err)
	}
	if summary.EventCount != 2 || summary.BytesDownloaded != 123 || summary.BytesUploaded != 50 {
		t.Fatalf("unexpected summary output: %+v", summary)
	}
	if summary.Freshness == nil || summary.Freshness.IsStale {
		t.Fatalf("expected summary output to include freshness metadata, got %+v", summary.Freshness)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "metrics", "transfers", "breakdown", "--organization", "syfon", "--project", "e2e", "--group-by", "user", "--provider", "file", "--bucket", "syfon-bucket", "--from", syncFrom, "--to", syncTo)
	if err != nil {
		t.Fatalf("metrics transfers breakdown command failed: %v output=%s", err, out)
	}
	var breakdown struct {
		GroupBy   string                                `json:"group_by"`
		Data      []models.TransferAttributionBreakdown `json:"data"`
		Freshness *models.TransferMetricsFreshness      `json:"freshness"`
	}
	if err := json.Unmarshal([]byte(out), &breakdown); err != nil {
		t.Fatalf("decode breakdown output %q: %v", out, err)
	}
	if breakdown.GroupBy != "user" || len(breakdown.Data) != 1 || breakdown.Data[0].Key != "user@example.com" || breakdown.Data[0].BytesDownloaded != 123 {
		t.Fatalf("unexpected breakdown output: %+v", breakdown)
	}
	if breakdown.Freshness == nil || breakdown.Freshness.IsStale {
		t.Fatalf("expected breakdown output to include freshness metadata, got %+v", breakdown.Freshness)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "metrics", "transfers", "billing", "--organization", "syfon", "--project", "e2e", "--user", "user@example.com", "--from", syncFrom, "--to", syncTo)
	if err != nil {
		t.Fatalf("metrics transfers billing command failed: %v output=%s", err, out)
	}
	var billing struct {
		Summary          models.TransferAttributionSummary     `json:"summary"`
		StorageLocations []models.TransferAttributionBreakdown `json:"storage_locations"`
		Files            []models.TransferAttributionBreakdown `json:"files"`
	}
	if err := json.Unmarshal([]byte(out), &billing); err != nil {
		t.Fatalf("decode billing output %q: %v", out, err)
	}
	if billing.Summary.BytesDownloaded != 123 || billing.Summary.BytesUploaded != 50 {
		t.Fatalf("unexpected billing summary: %+v", billing.Summary)
	}
	if len(billing.StorageLocations) != 1 || billing.StorageLocations[0].Provider != "file" || billing.StorageLocations[0].Bucket != "syfon-bucket" {
		t.Fatalf("expected storage-location billing row, got %+v", billing.StorageLocations)
	}
	if len(billing.Files) != 2 {
		t.Fatalf("expected file-level billing rows, got %+v", billing.Files)
	}
}

func providerDownloadEventFromObject(eventID string, obj *models.InternalObject, bytes int64) models.ProviderTransferEvent {
	ev := models.ProviderTransferEvent{
		ProviderEventID:  eventID,
		Direction:        models.ProviderTransferDirectionDownload,
		EventTime:        time.Now().UTC(),
		Provider:         "s3",
		Bucket:           "syfon-bucket",
		BytesTransferred: bytes,
		HTTPMethod:       "GET",
		HTTPStatus:       200,
	}
	if obj == nil {
		return ev
	}
	ev.ObjectID = obj.Id
	ev.ObjectSize = obj.Size
	for _, checksum := range obj.Checksums {
		if checksum.Type == "sha256" {
			ev.SHA256 = checksum.Checksum
			break
		}
	}
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl != nil && strings.TrimSpace(am.AccessUrl.Url) != "" {
				ev.StorageURL = strings.TrimSpace(am.AccessUrl.Url)
				if parsed, err := url.Parse(ev.StorageURL); err == nil {
					if parsed.Scheme != "" {
						ev.Provider = common.ProviderFromScheme(parsed.Scheme)
					}
					ev.Bucket = parsed.Host
					ev.ObjectKey = strings.TrimLeft(parsed.Path, "/")
				}
				break
			}
		}
	}
	return ev
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
	URL        string
	StorageDir string
	DB         db.DatabaseInterface
	app        *fiber.App
	ln         net.Listener
}

func (s *fiberTestServer) Close() {
	_ = s.app.Shutdown()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func newSyfonTestServer(t *testing.T) *fiberTestServer {
	t.Helper()

	kek := filepath.Join(t.TempDir(), ".syfon-credential-kek")
	t.Setenv("DRS_CREDENTIAL_LOCAL_KEY_FILE", kek)
	t.Setenv("SYFON_CREDENTIAL_KEK_DIR", kek)
	// start server...

	storageDir := t.TempDir()

	database := db.NewInMemoryDB()
	if err := database.SaveS3Credential(context.Background(), &models.S3Credential{
		Bucket:   "syfon-bucket",
		Provider: "file",
		Endpoint: storageDir,
	}); err != nil {
		t.Fatalf("save test credential: %v", err)
	}

	uM := urlmanager.NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})
	fSigner, _ := file.NewFileSigner(storageDir)
	uM.RegisterSigner(common.FileProvider, fSigner)

	app := fiber.New()
	app.Get(config.RouteHealthz, func(c fiber.Ctx) error {
		return c.SendString("OK")
	})
	api := app.Group("/")
	om := core.NewObjectManager(database, uM)

	drsAPI := api.Group("/ga4gh/drs/v1")
	drsapi.RegisterDRSRoutes(drsAPI, om)
	docs.RegisterSwaggerRoutes(app)
	metrics.RegisterMetricsRoutes(api, database)
	internaldrs.RegisterInternalRoutes(api, om)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("listen not available in this environment: %v", err)
	}
	go func() {
		_ = app.Listener(ln)
	}()

	return &fiberTestServer{
		URL:        "http://" + ln.Addr().String(),
		StorageDir: storageDir,
		DB:         database,
		app:        app,
		ln:         ln,
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
	out, err := executeRootCommand(t, "--server", server.URL, "upload", "--file", srcPath, "--did", uploadDID, "--org", "syfon", "--project", "e2e")
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

	externalSource := filepath.Join(server.StorageDir, "existing-url-source.txt")
	externalData := []byte("syfon add-url payload")
	if err := os.WriteFile(externalSource, externalData, 0o644); err != nil {
		t.Fatalf("write external source file: %v", err)
	}
	addURLDID := uuid.NewString()
	out, err = executeRootCommand(
		t,
		"--server", server.URL,
		"add-url",
		"--did", addURLDID,
		"--url", "s3://syfon-bucket/"+filepath.Base(externalSource),
		"--org", "syfon", "--project", "e2e",
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

	uploadObj, err := server.DB.GetObject(context.Background(), uploadDID)
	if err != nil {
		t.Fatalf("fetch upload object for provider metrics: %v", err)
	}
	addURLObj, err := server.DB.GetObject(context.Background(), addURLDID)
	if err != nil {
		t.Fatalf("fetch add-url object for provider metrics: %v", err)
	}
	providerEvents := []models.ProviderTransferEvent{
		providerDownloadEventFromObject("cli-provider-download-1", uploadObj, int64(len(srcData))),
		providerDownloadEventFromObject("cli-provider-download-2", addURLObj, int64(len(externalData))),
	}
	if err := server.DB.RecordProviderTransferEvents(context.Background(), providerEvents); err != nil {
		t.Fatalf("record provider transfer metrics: %v", err)
	}

	out, err = executeRootCommand(
		t,
		"--server", server.URL,
		"metrics", "transfers", "summary",
		"--organization", "syfon",
		"--project", "e2e",
		"--direction", models.ProviderTransferDirectionDownload,
	)
	if err != nil {
		t.Fatalf("metrics transfers summary command failed: %v output=%s", err, out)
	}
	var accessSummary models.TransferAttributionSummary
	if err := json.Unmarshal([]byte(out), &accessSummary); err != nil {
		t.Fatalf("decode metrics summary output %q: %v", out, err)
	}
	if accessSummary.DownloadEventCount < 1 || accessSummary.EventCount < 1 {
		t.Fatalf("expected provider transfer metrics for downloads, got %+v", accessSummary)
	}
	if accessSummary.BytesDownloaded <= 0 {
		t.Fatalf("expected provider bytes to cover downloaded payloads, got %+v", accessSummary)
	}
	if accessSummary.Freshness == nil || accessSummary.Freshness.IsStale {
		t.Fatalf("expected signed-url billing metrics to include non-stale freshness metadata, got %+v", accessSummary.Freshness)
	}

	out, err = executeRootCommand(
		t,
		"--server", server.URL,
		"metrics", "transfers", "breakdown",
		"--organization", "syfon",
		"--project", "e2e",
		"--direction", models.ProviderTransferDirectionDownload,
		"--group-by", "scope",
	)
	if err != nil {
		t.Fatalf("metrics transfers breakdown command failed: %v output=%s", err, out)
	}
	var accessBreakdown struct {
		GroupBy string                                `json:"group_by"`
		Data    []models.TransferAttributionBreakdown `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &accessBreakdown); err != nil {
		t.Fatalf("decode metrics breakdown output %q: %v", out, err)
	}
	if accessBreakdown.GroupBy != "scope" || len(accessBreakdown.Data) == 0 {
		t.Fatalf("expected scoped transfer breakdown, got %+v", accessBreakdown)
	}
	foundScope := false
	for _, row := range accessBreakdown.Data {
		if row.Organization == "syfon" && row.Project == "e2e" && row.EventCount >= 1 {
			foundScope = true
			break
		}
	}
	if !foundScope {
		t.Fatalf("expected syfon/e2e transfer metrics row, got %+v", accessBreakdown.Data)
	}
}
