package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

func TestWriteLFSErrorPreservesLFSContentType(t *testing.T) {
	app := fiber.New()
	app.Get("/error", func(c fiber.Ctx) error {
		return writeLFSError(c, http.StatusTooManyRequests, "rate limit exceeded", false)
	})

	response, err := app.Test(httptest.NewRequest(http.MethodGet, "/error", nil))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTooManyRequests)
	}
	if got := response.Header.Get("Content-Type"); got != "application/vnd.git-lfs+json" {
		t.Fatalf("Content-Type = %q, want application/vnd.git-lfs+json", got)
	}
}

func TestLFSClientKeyUsesCompleteAuthorizationWithoutRetainingIt(t *testing.T) {
	prefix := strings.Repeat("shared-authorization-prefix-", 4)
	first := prefix + "first-secret-suffix"
	second := prefix + "second-secret-suffix"
	keys := make([]string, 0, 2)
	app := fiber.New()
	app.Post("/key", func(c fiber.Ctx) error {
		keys = append(keys, requestClientKey(c))
		return c.SendStatus(http.StatusNoContent)
	})
	for _, authorization := range []string{first, second} {
		request := httptest.NewRequest(http.MethodPost, "/key", nil)
		request.Header.Set("Authorization", authorization)
		response, err := app.Test(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("authorization keys = %v, want distinct keys", keys)
	}
	for _, key := range keys {
		if strings.Contains(key, prefix) || strings.Contains(key, "secret-suffix") {
			t.Fatalf("limiter key retained raw authorization material: %q", key)
		}
	}
}

func TestLFSRequestLimitStateIsOwnedByMiddlewareInstance(t *testing.T) {
	status := func(middleware lfsapi.StrictMiddlewareFunc, authorization string) int {
		t.Helper()
		app := fiber.New()
		handler := middleware(func(fiber.Ctx, interface{}) (interface{}, error) {
			return struct{}{}, nil
		}, "LfsUploadProxy")
		app.Post("/upload", func(c fiber.Ctx) error {
			result, err := handler(c, nil)
			if err != nil {
				return err
			}
			if result != nil {
				return c.SendStatus(http.StatusNoContent)
			}
			return nil
		})
		request := httptest.NewRequest(http.MethodPost, "/upload", nil)
		request.Header.Set("Authorization", authorization)
		response, err := app.Test(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	options := LFSOptions{RequestLimitPerMinute: 1}
	authorization := "Bearer middleware-owner-regression"
	firstRuntime := lfsRequestMiddleware(options)
	secondRuntime := lfsRequestMiddleware(options)
	if got := status(firstRuntime, authorization); got != http.StatusNoContent {
		t.Fatalf("first runtime status = %d, want 204", got)
	}
	if got := status(secondRuntime, authorization); got != http.StatusNoContent {
		t.Fatalf("second runtime inherited limiter state: status = %d, want 204", got)
	}
	if got := status(firstRuntime, authorization); got != http.StatusTooManyRequests {
		t.Fatalf("first runtime second request status = %d, want 429", got)
	}
}

func TestLFSLimiterBoundsHighCardinalityAndResetsWindows(t *testing.T) {
	limiter := newLFSLimiter(3)
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 100; i++ {
		if !limiter.allowRequest(fmt.Sprintf("client-%d", i), now, 1) {
			t.Fatalf("new client %d was unexpectedly limited", i)
		}
	}
	if len(limiter.clients) != 3 {
		t.Fatalf("tracked clients = %d, want capacity 3", len(limiter.clients))
	}
	if !limiter.allowRequest("client-99", now.Add(time.Minute), 1) {
		t.Fatal("new minute did not reset the request window")
	}
	if limiter.allowRequest("client-99", now.Add(time.Minute), 1) {
		t.Fatal("second request in the reset window exceeded its quota")
	}
}

func TestLFSBandwidthLimitCannotOverflow(t *testing.T) {
	limiter := newLFSLimiter(1)
	now := time.Unix(1_700_000_000, 0)
	if !limiter.allowBandwidth("client", now, 1, math.MaxInt64) {
		t.Fatal("first byte was unexpectedly limited")
	}
	if limiter.allowBandwidth("client", now, math.MaxInt64, math.MaxInt64) {
		t.Fatal("bandwidth counter overflow bypassed the limit")
	}
}

func TestLFSLimiterBoundsConcurrentClients(t *testing.T) {
	limiter := newLFSLimiter(8)
	now := time.Unix(1_700_000_000, 0)
	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			key := fmt.Sprintf("client-%d", i)
			limiter.allowRequest(key, now, 10)
			limiter.allowBandwidth(key, now, 1, 10)
		}(i)
	}
	wait.Wait()
	if len(limiter.clients) > 8 {
		t.Fatalf("tracked clients = %d, exceeds capacity 8", len(limiter.clients))
	}
}

func TestLFSBatchDownloadUsesTransferAndUsagePorts(t *testing.T) {
	oid := strings.Repeat("a", 64)
	ports := newLFSTestPorts(t,
		map[string]*drs.DrsObject{
			oid: {
				Id:        oid,
				Size:      10,
				Checksums: []drs.Checksum{{Type: "sha256", Checksum: oid}},
				AccessMethods: &[]drs.AccessMethod{{
					Type:      "s3",
					AccessUrl: &drs.AccessURL{Url: "s3://bucket/" + oid},
				}},
			},
		},
		map[string]buckets.Credential{"bucket": {Bucket: "bucket"}},
	)
	storageFake := &lfsTestStorage{}
	router := newLFSTestRouter(ports, storageFake, defaultLFSOptions())
	body, _ := json.Marshal(map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": oid, "size": 10}},
	})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(body))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("batch status = %d body=%s", response.Code, response.Body.String())
	}
	var payload lfsapi.BatchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if len(payload.Objects) != 1 || payload.Objects[0].Actions == nil || payload.Objects[0].Actions.Download == nil {
		t.Fatalf("download actions = %+v", payload.Objects)
	}
	if len(ports.downloads) != 1 || ports.downloads[0] != oid {
		t.Fatalf("download counters = %v", ports.downloads)
	}
	if len(ports.transferEvents) != 1 || ports.transferEvents[0].EventType != usage.TransferEventAccessIssued {
		t.Fatalf("transfer events = %+v", ports.transferEvents)
	}
}

func TestLFSMetadataVerifyPreservesPendingPopBeforeRegister(t *testing.T) {
	payload := []byte("hello lfs!!!")
	oid := lfsTestOID(payload)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	router := newLFSTestRouter(ports, &lfsTestStorage{}, defaultLFSOptions())
	metadata, _ := json.Marshal(map[string]any{"candidates": []map[string]any{{
		"name": "object.bin", "size": len(payload),
		"checksums":      []map[string]any{{"type": "sha256", "checksum": oid}},
		"access_methods": []map[string]any{{"type": "s3", "access_url": map[string]any{"url": "s3://bucket/" + oid}}},
	}}})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/metadata", bytes.NewReader(metadata))
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metadata status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	entry, ok := ports.pending[oid]
	if !ok || entry.CreatedAt.IsZero() || entry.ExpiresAt.Sub(entry.CreatedAt) != transferlfs.PendingMetadataTTL {
		t.Fatalf("pending metadata timestamps = %+v", entry)
	}
	verify, _ := json.Marshal(map[string]any{"oid": oid, "size": len(payload)})
	request = httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(verify))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err := router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNotFound {
		response.Body.Close()
		t.Fatalf("verify before upload status = %d, want 404", response.StatusCode)
	}
	response.Body.Close()
	if _, ok := ports.pending[oid]; !ok {
		t.Fatal("pending metadata was consumed before upload")
	}
	storageFake := &lfsTestStorage{}
	router = newLFSTestRouter(ports, storageFake, defaultLFSOptions())
	request = httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err = router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("upload status = %d body=%s", response.StatusCode, body)
	}
	response.Body.Close()
	request = httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(verify))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err = router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		t.Fatalf("verify after upload status = %d", response.StatusCode)
	}
	response.Body.Close()
	if _, ok := ports.pending[oid]; ok {
		t.Fatal("pending metadata was not consumed after verified upload")
	}
}

func lfsTestOID(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func TestLFSUploadProxyPreservesOpaqueMultipartAndPartOrder(t *testing.T) {
	payload := []byte("payload")
	oid := lfsTestOID(payload)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	storageFake := &lfsTestStorage{}
	deps := newLFSTestDependencies(ports, storageFake)
	storageFake.uploadPart = func(content []byte) (string, error) {
		if string(content) != "payload" {
			t.Fatalf("multipart content = %q", content)
		}
		return "etag", nil
	}
	server := &lfsServer{service: deps, opts: defaultLFSOptions()}
	response, err := server.LfsUploadProxy(context.Background(), lfsapi.LfsUploadProxyRequestObject{
		Oid:  oid,
		Body: bytes.NewReader(payload),
	})
	if err != nil {
		t.Fatalf("upload proxy error: %v", err)
	}
	if _, ok := response.(lfsapi.LfsUploadProxy200Response); !ok {
		t.Fatalf("upload proxy response = %T, want 200", response)
	}
	want := storage.Target{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: oid, CanonicalURL: "s3://bucket/" + oid, LookupCandidates: []string{"bucket"}}
	if !reflect.DeepEqual(storageFake.initTarget, want) {
		t.Fatalf("multipart init target = %+v", storageFake.initTarget)
	}
	if storageFake.partRequest.UploadID != "opaque-upload-id" || storageFake.partRequest.PartNumber != 1 {
		t.Fatalf("multipart part request = %+v", storageFake.partRequest)
	}
	if storageFake.complete.UploadID != "opaque-upload-id" || len(storageFake.complete.Parts) != 1 || storageFake.complete.Parts[0].ETag != "etag" {
		t.Fatalf("multipart completion = %+v", storageFake.complete)
	}
}

func TestLFSUploadProxyRedactsSignedPartErrorsFromLog(t *testing.T) {
	const signedURL = "https://part-user:part-password@parts.example/upload?X-Amz-Signature=synthetic-part-secret#synthetic-part-fragment"
	payload := []byte("payload")
	oid := lfsTestOID(payload)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	storageFake := &lfsTestStorage{partLocation: signedURL}
	storageFake.uploadPart = func([]byte) (string, error) {
		return "", fmt.Errorf("wrapped transport failure: %w", &url.Error{
			Op:  http.MethodPut,
			URL: signedURL,
			Err: errors.New("connection refused"),
		})
	}
	server := &lfsServer{service: newLFSTestDependencies(ports, storageFake), opts: defaultLFSOptions()}

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	response, err := server.LfsUploadProxy(context.Background(), lfsapi.LfsUploadProxyRequestObject{
		Oid:  oid,
		Body: bytes.NewReader(payload),
	})
	if err != nil {
		t.Fatalf("upload proxy returned error: %v", err)
	}
	internal, ok := response.(lfsapi.LfsUploadProxy500TextResponse)
	if !ok || string(internal) != http.StatusText(http.StatusInternalServerError) {
		t.Fatalf("upload proxy response = %#v, want generic 500 response", response)
	}
	logText := logs.String()
	if !strings.Contains(logText, "failed uploading multipart part 1") || !strings.Contains(logText, "https://parts.example/upload") {
		t.Fatalf("server log omitted failed part context: %s", logText)
	}
	for _, secret := range []string{"part-user", "part-password", "X-Amz-Signature", "synthetic-part-secret", "synthetic-part-fragment"} {
		if strings.Contains(logText, secret) || strings.Contains(string(internal), secret) {
			t.Fatalf("signed part error leaked %q to log or response", secret)
		}
	}
}

func TestLFSUploadStreamsBodiesLargerThanFiberDefaultLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), fiber.DefaultBodyLimit+1024)
	oid := lfsTestOID(payload)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	storageFake := &lfsTestStorage{}
	var streamed bool
	var receivedBytes int
	storageFake.uploadPart = func(content []byte) (string, error) {
		receivedBytes += len(content)
		if !bytes.Equal(content, payload) {
			return "", fmt.Errorf("multipart body differed from request payload")
		}
		return "etag", nil
	}
	app := fiber.New(fiber.Config{StreamRequestBody: true})
	app.Use(func(c fiber.Ctx) error {
		if c.Method() == http.MethodPut {
			streamed = c.Request().IsBodyStream()
		}
		return c.Next()
	})
	registerLFSRoutes(app, newLFSTestDependencies(ports, storageFake), defaultLFSOptions())

	request := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("upload status = %d body=%s", response.StatusCode, body)
	}
	if !streamed {
		t.Fatal("Fiber did not expose the request body as a stream")
	}
	if receivedBytes != len(payload) {
		t.Fatalf("multipart bytes = %d, want %d", receivedBytes, len(payload))
	}
	receipt, ok := ports.receipts[oid]
	if !ok || receipt.OID != oid || receipt.SHA256 != oid || receipt.Size != int64(len(payload)) {
		t.Fatalf("upload receipt = %+v, want size %d and matching SHA-256", receipt, len(payload))
	}
}

func TestLFSUploadHashMismatchAbortsAndCannotVerify(t *testing.T) {
	expectedPayload := []byte("expected bytes")
	wrongPayload := []byte("different data")
	oid := lfsTestOID(expectedPayload)
	ports := newLFSTestPorts(t, nil, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	storageFake := &lfsTestStorage{}
	router := newLFSTestRouter(ports, storageFake, defaultLFSOptions())
	service := newLFSTestDependencies(ports, storageFake)
	size := int64(len(expectedPayload))
	typeName := "s3"
	location := "s3://bucket/" + oid
	if err := service.Stage(context.Background(), []lfsapi.DrsObjectCandidate{{
		Size:      &size,
		Checksums: &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{
			Type:      &typeName,
			AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location},
		}},
	}}, transferlfs.PendingMetadataTTL); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader(wrongPayload))
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("hash-mismatched upload status = %d, want 400", response.StatusCode)
	}
	if len(storageFake.aborted) != 1 || storageFake.complete.UploadID != "" {
		t.Fatalf("multipart cleanup: aborted=%+v completed=%+v", storageFake.aborted, storageFake.complete)
	}
	verify, _ := json.Marshal(map[string]any{"oid": oid, "size": len(expectedPayload)})
	request = httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(verify))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err = router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("verify after hash-mismatched upload status = %d, want 404", response.StatusCode)
	}
	if _, ok := ports.pending[oid]; !ok {
		t.Fatal("failed upload consumed staged metadata")
	}
}

func TestLFSBandwidthLimitChargesUploadedBytesNotBatchDeclarations(t *testing.T) {
	ports := newLFSTestPorts(t, nil, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
	options := defaultLFSOptions()
	options.BandwidthLimitBytesPerMinute = 6
	storageFake := &lfsTestStorage{}
	router := newLFSTestRouter(ports, storageFake, options)
	firstPayload := []byte("1234567")
	firstOID := lfsTestOID(firstPayload)
	batchBody, _ := json.Marshal(map[string]any{
		"operation": "upload",
		"objects":   []map[string]any{{"oid": firstOID, "size": 100}},
	})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(batchBody))
	request.Header.Set("Authorization", "Bearer lfs-byte-limit-test")
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err := router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("batch status = %d, want 200 despite declared size exceeding the byte limit", response.StatusCode)
	}

	request = httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+firstOID, bytes.NewReader(firstPayload))
	request.Header.Set("Authorization", "Bearer lfs-byte-limit-test")
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err = router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 509 {
		t.Fatalf("upload beyond actual byte limit status = %d body=%s aborted=%d receipts=%d, want 509", response.StatusCode, responseBody, len(storageFake.aborted), len(ports.receipts))
	}
}

func TestLFSTopLevelInternalErrorsDoNotExposeDetails(t *testing.T) {
	const detail = "postgres://user:secret@database"
	oid := strings.Repeat("e", 64)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{})
	ports.getErr = errors.New(detail)
	server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}

	response, err := server.LfsVerify(context.Background(), lfsapi.LfsVerifyRequestObject{
		Body: &lfsapi.LfsVerifyApplicationVndGitLfsPlusJSONRequestBody{Oid: oid, Size: 1},
	})
	if err != nil {
		t.Fatalf("verify returned error: %v", err)
	}
	internal, ok := response.(lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse)
	if !ok || internal.Message != http.StatusText(http.StatusInternalServerError) || strings.Contains(internal.Message, detail) {
		t.Fatalf("unexpected verify response: %#v", response)
	}

	ports.getErr = nil
	response507, err := server.LfsUploadProxy(context.Background(), lfsapi.LfsUploadProxyRequestObject{
		Oid:  oid,
		Body: strings.NewReader("payload"),
	})
	if err != nil {
		t.Fatalf("upload proxy returned error: %v", err)
	}
	insufficient, ok := response507.(lfsapi.LfsUploadProxy507TextResponse)
	if !ok || string(insufficient) != http.StatusText(http.StatusInsufficientStorage) {
		t.Fatalf("unexpected upload response: %#v", response507)
	}
}

func TestLFSBatchInternalErrorsDoNotExposeDetails(t *testing.T) {
	const detail = "credential secret leaked"
	internal := batchErrToObjectError(context.Background(), errors.New(detail), false)
	if internal.Code != http.StatusInternalServerError || internal.Message != http.StatusText(http.StatusInternalServerError) || strings.Contains(internal.Message, detail) {
		t.Fatalf("unexpected batch error: %+v", internal)
	}
	insufficient := batchErrToObjectError(context.Background(), errorapi.ErrBucketNotConfigured, false)
	if insufficient.Code != http.StatusInsufficientStorage || insufficient.Message != http.StatusText(http.StatusInsufficientStorage) {
		t.Fatalf("unexpected no-bucket error: %+v", insufficient)
	}
}

func TestLFSUploadProxyUsesCanonicalOIDForScopedTargets(t *testing.T) {
	payload := []byte("payload")
	oid := lfsTestOID(payload)
	newTransferService := func(ports *lfsTestServicePorts, storageFake *lfsTestStorage) *transfers.Service {
		return transfers.NewService(transfers.Dependencies{
			Objects:           objects.NewService(ports),
			Storage:           storageFake,
			Scopes:            lfsTestScopeReader{scopes: map[string]buckets.Scope{"org|project": {Organization: "org", ProjectID: "project", Bucket: "physical", PathPrefix: "project-prefix"}}},
			Credentials:       ports,
			Events:            ports,
			MultipartSessions: ports.Store,
		})
	}

	tests := []struct {
		name     string
		populate func(*lfsTestServicePorts)
	}{
		{
			name: "existing object",
			populate: func(ports *lfsTestServicePorts) {
				resources := []string{"/programs/org/projects/project"}
				methods := []drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://legacy/stale-key"}}}
				if err := ports.RegisterObjects(context.Background(), []drs.DrsObject{{
					Id:               "record-existing",
					Checksums:        []drs.Checksum{{Type: "sha256", Checksum: oid}},
					AccessMethods:    &methods,
					ControlledAccess: &resources,
				}}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "pending metadata",
			populate: func(ports *lfsTestServicePorts) {
				resources := []string{"/programs/org/projects/project"}
				typeName := "s3"
				url := "s3://legacy/stale-key"
				ports.pending[oid] = transferlfs.PendingMetadata{
					OID: oid,
					Candidate: lfsapi.DrsObjectCandidate{
						Checksums:        &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
						AccessMethods:    &[]lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}},
						ControlledAccess: &resources,
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ports := newLFSTestPorts(t, map[string]*drs.DrsObject{}, map[string]buckets.Credential{"physical": {Bucket: "physical"}})
			tt.populate(ports)
			storageFake := &lfsTestStorage{}
			deps := newLFSTestDependenciesWithTransfer(ports, storageFake, newTransferService(ports, storageFake))
			storageFake.uploadPart = func([]byte) (string, error) { return "etag", nil }
			server := &lfsServer{service: deps, opts: defaultLFSOptions()}

			response, err := server.LfsUploadProxy(context.Background(), lfsapi.LfsUploadProxyRequestObject{
				Oid:  oid,
				Body: bytes.NewReader(payload),
			})
			if err != nil {
				t.Fatalf("upload proxy error: %v", err)
			}
			if _, ok := response.(lfsapi.LfsUploadProxy200Response); !ok {
				t.Fatalf("upload proxy response = %T (%+v)", response, response)
			}
			want := storage.Target{Provider: "s3", LookupKey: "physical", PhysicalBucket: "physical", Key: "project-prefix/" + oid, Path: "/project-prefix/" + oid, CanonicalURL: "s3://physical/project-prefix/" + oid, LookupCandidates: []string{"physical"}}
			if !reflect.DeepEqual(storageFake.initTarget, want) {
				t.Fatalf("multipart init target = %+v, want %+v", storageFake.initTarget, want)
			}
		})
	}
}

func TestLFSBatchRejectsNegativeObjectSize(t *testing.T) {
	server := newLFSTestServerForNumericValidation(t)
	response, err := server.LfsBatch(context.Background(), lfsapi.LfsBatchRequestObject{
		Body: &lfsapi.LfsBatchApplicationVndGitLfsPlusJSONRequestBody{
			Operation: "upload",
			Objects:   []lfsapi.BatchRequestObject{{Oid: strings.Repeat("a", 64), Size: -1}},
		},
	})
	if err != nil {
		t.Fatalf("LfsBatch() error = %v", err)
	}
	batch, ok := response.(lfsapi.LfsBatch200ApplicationVndGitLfsPlusJSONResponse)
	if !ok || len(batch.Objects) != 1 {
		t.Fatalf("LfsBatch() response = %#v", response)
	}
	if batch.Objects[0].Size != 0 || batch.Objects[0].Error == nil || batch.Objects[0].Error.Code != 400 {
		t.Fatalf("negative size response = %+v", batch.Objects[0])
	}
}

func TestLFSVerifyRejectsNegativeSize(t *testing.T) {
	server := newLFSTestServerForNumericValidation(t)
	response, err := server.LfsVerify(context.Background(), lfsapi.LfsVerifyRequestObject{
		Body: &lfsapi.LfsVerifyApplicationVndGitLfsPlusJSONRequestBody{Oid: strings.Repeat("b", 64), Size: -1},
	})
	if err != nil {
		t.Fatalf("LfsVerify() error = %v", err)
	}
	if invalid, ok := response.(lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse); !ok || invalid.Message != "size must be non-negative" {
		t.Fatalf("negative size response = %#v", response)
	}
}

func TestLFSVerifyRejectsRecordedSizeMismatch(t *testing.T) {
	oid := strings.Repeat("f", 64)
	ports := newLFSTestPorts(t, map[string]*drs.DrsObject{
		oid: {Id: oid, Size: 12},
	}, nil)
	server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}
	response, err := server.LfsVerify(context.Background(), lfsapi.LfsVerifyRequestObject{
		Body: &lfsapi.LfsVerifyApplicationVndGitLfsPlusJSONRequestBody{Oid: oid, Size: 11},
	})
	if err != nil {
		t.Fatalf("LfsVerify() error = %v", err)
	}
	if invalid, ok := response.(lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse); !ok || !strings.Contains(invalid.Message, "size mismatch") {
		t.Fatalf("recorded size mismatch response = %#v", response)
	}
}

func TestLFSStageMetadataRejectsNegativeCandidateSize(t *testing.T) {
	server := newLFSTestServerForNumericValidation(t)
	size := int64(-1)
	response, err := server.LfsStageMetadata(context.Background(), lfsapi.LfsStageMetadataRequestObject{
		JSONBody: &lfsapi.LfsStageMetadataJSONRequestBody{
			Candidates: []lfsapi.DrsObjectCandidate{{Size: &size}},
		},
	})
	if err != nil {
		t.Fatalf("LfsStageMetadata() error = %v", err)
	}
	if invalid, ok := response.(lfsapi.LfsStageMetadata400JSONResponse); !ok || invalid.Message != "candidate[0] size must be non-negative" {
		t.Fatalf("negative candidate size response = %#v", response)
	}
}

func TestLFSStageMetadataUsesRequestedTTL(t *testing.T) {
	oid := strings.Repeat("a", 64)
	ttlSeconds := int64(45 * 60)
	typeName, url := "s3", "s3://bucket/"+oid
	ports := newLFSTestPorts(t, nil, nil)
	router := newLFSTestRouter(ports, &lfsTestStorage{}, defaultLFSOptions())
	metadata, err := json.Marshal(lfsapi.MetadataSubmitRequest{
		Candidates: []lfsapi.DrsObjectCandidate{{
			Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
			AccessMethods: &[]lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}},
		}},
		TtlSeconds: &ttlSeconds,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/metadata", bytes.NewReader(metadata))
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err := router.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("metadata status = %d body=%s", response.StatusCode, body)
	}
	entry, ok := ports.pending[oid]
	if !ok || entry.ExpiresAt.Sub(entry.CreatedAt) != time.Duration(ttlSeconds)*time.Second {
		t.Fatalf("custom-TTL pending metadata = %+v, want TTL %s", entry, time.Duration(ttlSeconds)*time.Second)
	}
}

func TestLFSStageMetadataRejectsOutOfRangeTTL(t *testing.T) {
	for _, test := range []struct {
		name string
		ttl  int64
	}{
		{name: "zero", ttl: 0},
		{name: "negative", ttl: -1},
		{name: "above maximum", ttl: int64((24*time.Hour)/time.Second) + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ports := newLFSTestPorts(t, nil, nil)
			server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}
			ttlSeconds := test.ttl
			oid := strings.Repeat("a", 64)
			typeName, url := "s3", "s3://bucket/"+oid
			response, err := server.LfsStageMetadata(context.Background(), lfsapi.LfsStageMetadataRequestObject{
				JSONBody: &lfsapi.MetadataSubmitRequest{
					Candidates: []lfsapi.DrsObjectCandidate{{
						Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
						AccessMethods: &[]lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}},
					}},
					TtlSeconds: &ttlSeconds,
				},
			})
			if err != nil {
				t.Fatalf("LfsStageMetadata() error = %v", err)
			}
			if invalid, ok := response.(lfsapi.LfsStageMetadata400JSONResponse); !ok || !strings.Contains(invalid.Message, "ttl_seconds") {
				t.Fatalf("out-of-range TTL response = %#v, want 400 naming ttl_seconds", response)
			}
			if len(ports.pending) != 0 {
				t.Fatalf("out-of-range TTL staged metadata: %+v", ports.pending)
			}
		})
	}
}

func TestLFSNegativeVerifySizeUsesHTTPErrorContract(t *testing.T) {
	ports := newLFSTestPorts(t, nil, nil)
	router := newLFSTestRouter(ports, &lfsTestStorage{}, defaultLFSOptions())
	body, _ := json.Marshal(map[string]any{"oid": strings.Repeat("c", 64), "size": -1})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/verify", strings.NewReader(string(body)))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response, err := router.app.Test(request)
	if err != nil {
		t.Fatalf("negative verify request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("negative verify status = %d, want 400", response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); got != "application/vnd.git-lfs+json" {
		t.Fatalf("negative verify media type = %q", got)
	}
}

func TestLFSZeroSizesRemainAccepted(t *testing.T) {
	oid := strings.Repeat("d", 64)
	t.Run("batch", func(t *testing.T) {
		ports := newLFSTestPorts(t, map[string]*drs.DrsObject{
			oid: {
				Id:        oid,
				Checksums: []drs.Checksum{{Type: "sha256", Checksum: oid}},
				AccessMethods: &[]drs.AccessMethod{{
					Type:      "s3",
					AccessUrl: &drs.AccessURL{Url: "s3://bucket/" + oid},
				}},
			},
		}, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
		server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}
		response, err := server.LfsBatch(context.Background(), lfsapi.LfsBatchRequestObject{
			Body: &lfsapi.LfsBatchApplicationVndGitLfsPlusJSONRequestBody{
				Operation: "download",
				Objects:   []lfsapi.BatchRequestObject{{Oid: oid, Size: 0}},
			},
		})
		if err != nil {
			t.Fatalf("LfsBatch() error = %v", err)
		}
		batch := response.(lfsapi.LfsBatch200ApplicationVndGitLfsPlusJSONResponse)
		if batch.Objects[0].Error != nil || batch.Objects[0].Size != 0 {
			t.Fatalf("zero batch size response = %+v", batch.Objects[0])
		}
	})

	t.Run("verify", func(t *testing.T) {
		ports := newLFSTestPorts(t, map[string]*drs.DrsObject{oid: {Id: oid}}, nil)
		server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}
		response, err := server.LfsVerify(context.Background(), lfsapi.LfsVerifyRequestObject{
			Body: &lfsapi.LfsVerifyApplicationVndGitLfsPlusJSONRequestBody{Oid: oid, Size: 0},
		})
		if err != nil {
			t.Fatalf("LfsVerify() error = %v", err)
		}
		if _, ok := response.(lfsapi.LfsVerify200Response); !ok {
			t.Fatalf("zero verify response = %#v", response)
		}
	})

	t.Run("stage metadata", func(t *testing.T) {
		size := int64(0)
		typ := "s3"
		url := "s3://bucket/" + oid
		ports := newLFSTestPorts(t, nil, map[string]buckets.Credential{"bucket": {Bucket: "bucket"}})
		server := &lfsServer{service: newLFSTestDependencies(ports, &lfsTestStorage{}), opts: defaultLFSOptions()}
		response, err := server.LfsStageMetadata(context.Background(), lfsapi.LfsStageMetadataRequestObject{
			JSONBody: &lfsapi.LfsStageMetadataJSONRequestBody{Candidates: []lfsapi.DrsObjectCandidate{{
				Size:      &size,
				Checksums: &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
				AccessMethods: &[]lfsapi.AccessMethod{{
					Type:      &typ,
					AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url},
				}},
			}}},
		})
		if err != nil {
			t.Fatalf("LfsStageMetadata() error = %v", err)
		}
		if staged, ok := response.(lfsapi.LfsStageMetadata200JSONResponse); !ok || staged.Staged != 1 {
			t.Fatalf("zero metadata size response = %#v", response)
		}
	})
}
