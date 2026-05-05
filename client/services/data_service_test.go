package services

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

func TestDataServiceOperationsAndTransferHelpers(t *testing.T) {
	t.Parallel()

	var (
		lastUploadBlank     internalapi.InternalUploadBlankRequest
		lastUploadBulk      internalapi.InternalUploadBulkRequest
		lastMultipartInit   internalapi.InternalMultipartInitRequest
		lastMultipartUpload internalapi.InternalMultipartUploadRequest
		lastMultipartDone   internalapi.InternalMultipartCompleteRequest
		uploadURLQuery      url.Values
		downloadURLQuery    url.Values
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/data/upload":
			if err := json.NewDecoder(r.Body).Decode(&lastUploadBlank); err != nil {
				t.Fatalf("Decode upload blank body returned error: %v", err)
			}
			guid := "guid-blank"
			bucket := "bucket-from-blank"
			if lastUploadBlank.Guid != nil && *lastUploadBlank.Guid == "bucketless-guid" {
				guid = "bucketless-guid"
			}
			writeJSON(t, w, http.StatusCreated, internalapi.InternalUploadBlankOutput{Guid: &guid, Bucket: &bucket})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/data/upload/"):
			uploadURLQuery = r.URL.Query()
			id := path.Base(r.URL.Path)
			if id == "missing-upload-url" {
				writeJSON(t, w, http.StatusOK, internalapi.InternalSignedURL{})
				return
			}
			if id == "upload-error" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			signed := "https://upload.example/" + id
			writeJSON(t, w, http.StatusOK, internalapi.InternalSignedURL{Url: &signed})
		case r.Method == http.MethodPost && r.URL.Path == "/data/upload/bulk":
			if err := json.NewDecoder(r.Body).Decode(&lastUploadBulk); err != nil {
				t.Fatalf("Decode upload bulk body returned error: %v", err)
			}
			writeJSON(t, w, http.StatusOK, internalapi.InternalUploadBulkOutput{})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/data/download/"):
			downloadURLQuery = r.URL.Query()
			id := path.Base(r.URL.Path)
			if id == "missing-download-url" {
				writeJSON(t, w, http.StatusOK, internalapi.InternalSignedURL{})
				return
			}
			signed := "https://download.example/" + id
			writeJSON(t, w, http.StatusOK, internalapi.InternalSignedURL{Url: &signed})
		case r.Method == http.MethodPost && r.URL.Path == "/data/multipart/init":
			if err := json.NewDecoder(r.Body).Decode(&lastMultipartInit); err != nil {
				t.Fatalf("Decode multipart init body returned error: %v", err)
			}
			uploadID := "upload-id"
			guid := "multipart-guid"
			writeJSON(t, w, http.StatusOK, internalapi.InternalMultipartInitOutput{UploadId: &uploadID, Guid: &guid})
		case r.Method == http.MethodPost && r.URL.Path == "/data/multipart/upload":
			if err := json.NewDecoder(r.Body).Decode(&lastMultipartUpload); err != nil {
				t.Fatalf("Decode multipart upload body returned error: %v", err)
			}
			presigned := "https://parts.example/upload"
			writeJSON(t, w, http.StatusOK, internalapi.InternalMultipartUploadOutput{PresignedUrl: &presigned})
		case r.Method == http.MethodPost && r.URL.Path == "/data/multipart/complete":
			if err := json.NewDecoder(r.Body).Decode(&lastMultipartDone); err != nil {
				t.Fatalf("Decode multipart complete body returned error: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && r.URL.Path == "/index/delete-me":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/index/delete-fail":
			w.WriteHeader(http.StatusTeapot)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	requester := &recordingRequester{response: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{`"etag-1"`}}, Body: io.NopCloser(strings.NewReader("ok"))}}
	service := NewDataService(mustInternalClient(t, server.URL), requester, discardLogger(), nil)
	ctx := context.Background()

	if out, err := service.UploadBlank(ctx, internalapi.InternalUploadBlankRequest{Guid: ptrString("guid-blank")}); err != nil || out.Guid == nil || *out.Guid != "guid-blank" {
		t.Fatalf("UploadBlank returned out=%+v err=%v", out, err)
	}
	if lastUploadBlank.Guid == nil || *lastUploadBlank.Guid != "guid-blank" {
		t.Fatalf("unexpected upload blank request: %+v", lastUploadBlank)
	}

	if _, err := service.UploadURL(ctx, UploadURLRequest{FileID: "file-1", Bucket: "bucket-a", FileName: "name.txt", ExpiresIn: 60}); err != nil {
		t.Fatalf("UploadURL returned error: %v", err)
	}
	if uploadURLQuery.Get("bucket") != "bucket-a" || uploadURLQuery.Get("file_name") != "name.txt" || uploadURLQuery.Get("expires_in") != "60" {
		t.Fatalf("unexpected upload URL query values: %v", uploadURLQuery)
	}

	if _, err := service.ResolveUploadURL(ctx, "missing-upload-url", "name.txt", common.FileMetadata{}, "bucket-a"); err == nil || !strings.Contains(err.Error(), "response missing URL") {
		t.Fatalf("expected missing upload URL error, got %v", err)
	}
	if _, err := service.UploadURL(ctx, UploadURLRequest{FileID: "upload-error"}); err == nil {
		t.Fatal("expected upload URL error on non-200 status")
	}

	if _, err := service.UploadBulk(ctx, internalapi.InternalUploadBulkRequest{Requests: []internalapi.InternalUploadBulkItem{{FileId: "file-1"}}}); err != nil {
		t.Fatalf("UploadBulk returned error: %v", err)
	}
	if len(lastUploadBulk.Requests) != 1 || lastUploadBulk.Requests[0].FileId != "file-1" {
		t.Fatalf("unexpected upload bulk request: %+v", lastUploadBulk)
	}

	if _, err := service.DownloadURL(ctx, "file-2", 30, true); err != nil {
		t.Fatalf("DownloadURL returned error: %v", err)
	}
	if downloadURLQuery.Get("expires_in") != "30" || downloadURLQuery.Get("redirect") != "true" {
		t.Fatalf("unexpected download URL query values: %v", downloadURLQuery)
	}
	if _, err := service.ResolveDownloadURL(ctx, "missing-download-url", ""); err == nil || !strings.Contains(err.Error(), "response missing URL") {
		t.Fatalf("expected missing download URL error, got %v", err)
	}

	if got, err := service.ResolveDownloadURL(ctx, "file-2", ""); err != nil || got != "https://download.example/file-2" {
		t.Fatalf("ResolveDownloadURL returned got=%q err=%v", got, err)
	}
	if got, err := service.ResolveUploadURL(ctx, "file-1", "name.txt", common.FileMetadata{}, "bucket-a"); err != nil || got != "https://upload.example/file-1" {
		t.Fatalf("ResolveUploadURL returned got=%q err=%v", got, err)
	}
	if got, err := service.ResolveUploadURL(ctx, "bucketless-guid", "name.txt", common.FileMetadata{}, ""); err != nil || got != "https://upload.example/bucketless-guid" {
		t.Fatalf("ResolveUploadURL without bucket returned got=%q err=%v", got, err)
	}
	if uploadURLQuery.Get("bucket") != "bucket-from-blank" {
		t.Fatalf("expected bucket-from-blank to be forwarded after blank upload, got %v", uploadURLQuery)
	}

	uploadID, guid, err := service.InitMultipartUpload(ctx, "guid-a", "name.txt", "bucket-a")
	if err != nil || uploadID != "upload-id" || guid != "multipart-guid" {
		t.Fatalf("InitMultipartUpload returned uploadID=%q guid=%q err=%v", uploadID, guid, err)
	}
	if lastMultipartInit.Guid == nil || *lastMultipartInit.Guid != "guid-a" || lastMultipartInit.FileName == nil || *lastMultipartInit.FileName != "name.txt" {
		t.Fatalf("unexpected multipart init request: %+v", lastMultipartInit)
	}

	partURL, err := service.GetMultipartUploadURL(ctx, "guid-a", "upload-id", 3, "bucket-a")
	if err != nil || partURL != "https://parts.example/upload" {
		t.Fatalf("GetMultipartUploadURL returned url=%q err=%v", partURL, err)
	}
	if lastMultipartUpload.Key != "guid-a" || lastMultipartUpload.UploadId != "upload-id" || lastMultipartUpload.PartNumber != 3 {
		t.Fatalf("unexpected multipart upload request: %+v", lastMultipartUpload)
	}

	etag, err := service.MultipartPart(ctx, "guid-a", "upload-id", 3, bytes.NewReader([]byte("chunk-data")))
	if err != nil || etag != "etag-1" {
		t.Fatalf("MultipartPart returned etag=%q err=%v", etag, err)
	}
	if requester.method != http.MethodPut || string(requester.body) != "chunk-data" {
		t.Fatalf("unexpected upload request captured: method=%s body=%q", requester.method, requester.body)
	}

	parts := []transfer.MultipartPart{{PartNumber: 2, ETag: "etag-2"}, {PartNumber: 1, ETag: "etag-1"}}
	if err := service.MultipartComplete(ctx, "guid-a", "upload-id", parts); err != nil {
		t.Fatalf("MultipartComplete returned error: %v", err)
	}
	if len(lastMultipartDone.Parts) != 2 || lastMultipartDone.Parts[0].PartNumber != 2 || lastMultipartDone.Parts[1].ETag != "etag-1" {
		t.Fatalf("unexpected multipart completion payload: %+v", lastMultipartDone)
	}
	if err := service.CompleteMultipartUpload(ctx, "guid-a", "upload-id", []internalapi.InternalMultipartPart{{PartNumber: 1, ETag: "etag-1"}}, "bucket-a"); err != nil {
		t.Fatalf("CompleteMultipartUpload returned error: %v", err)
	}

	if _, err := service.MultipartInit(ctx, "guid-a"); err != nil {
		t.Fatalf("MultipartInit returned error: %v", err)
	}

	if got, err := service.DeleteFile(ctx, "delete-me"); err != nil || got != "delete-me" {
		t.Fatalf("DeleteFile returned got=%q err=%v", got, err)
	}
	if err := service.Delete(ctx, "delete-me"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := service.DeleteFile(ctx, "delete-fail"); err == nil {
		t.Fatal("expected delete failure on non-success status")
	}

	md, err := service.Stat(ctx, "file-2")
	if err != nil || md.Provider != "http" || md.MD5 != "https://download.example/file-2" || !md.AcceptRanges {
		t.Fatalf("Stat returned md=%+v err=%v", md, err)
	}

	transferRequester := &recordingRequester{response: &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("payload"))}}
	transferService := NewDataService(nil, transferRequester, discardLogger(), nil)
	resp, err := transferService.Download(ctx, "https://download.example/file-3", ptrInt64(3), ptrInt64(8))
	if err != nil || resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("Download returned resp=%v err=%v", resp, err)
	}
	if transferRequester.builder.Headers["Range"] != "bytes=3-8" {
		t.Fatalf("expected range header, got %+v", transferRequester.builder.Headers)
	}

	reader, err := transferService.GetReader(ctx, "https://download.example/file-3")
	if err != nil {
		t.Fatalf("GetReader returned error: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "payload" {
		t.Fatalf("unexpected reader payload %q", data)
	}

	transferRequester.response = &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ignored"))}
	if _, err := transferService.GetRangeReader(ctx, "https://download.example/file-3", 5, 3); err != transfer.ErrRangeIgnored {
		t.Fatalf("expected ErrRangeIgnored, got %v", err)
	}

	transferRequester.response = &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("abc"))}
	rc, err := transferService.GetRangeReader(ctx, "https://download.example/file-3", 1, 3)
	if err != nil {
		t.Fatalf("GetRangeReader returned error: %v", err)
	}
	defer rc.Close()
	rangeData, _ := io.ReadAll(rc)
	if string(rangeData) != "abc" {
		t.Fatalf("unexpected range payload %q", rangeData)
	}

	if service.Name() != "syfon-data-service" {
		t.Fatalf("unexpected service name %q", service.Name())
	}
	if service.Logger() == nil {
		t.Fatal("expected logger")
	}
	if err := service.Validate(ctx, "bucket-a"); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}
