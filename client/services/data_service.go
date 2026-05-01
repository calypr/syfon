package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/logs"
	"github.com/calypr/syfon/client/request"
	"github.com/calypr/syfon/client/transfer"
)

type DataService struct {
	gen       internalapi.ClientWithResponsesInterface
	requestor request.Requester
	logger    *logs.Gen3Logger
	drs       *DRSService
	uploadMu  sync.RWMutex
	uploads   map[string]string
}

func NewDataService(gen internalapi.ClientWithResponsesInterface, r request.Requester, l *logs.Gen3Logger, drs *DRSService) *DataService {
	return &DataService{
		gen:       gen,
		requestor: r,
		logger:    l,
		drs:       drs,
		uploads:   map[string]string{},
	}
}

func (d *DataService) UploadBlank(ctx context.Context, req internalapi.InternalUploadBlankRequest) (internalapi.InternalUploadBlankOutput, error) {
	resp, err := d.gen.InternalUploadBlankWithResponse(ctx, internalapi.InternalUploadBlankJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalUploadBlankOutput{}, err
	}
	if resp.JSON201 == nil {
		return internalapi.InternalUploadBlankOutput{}, fmt.Errorf("failed to upload blank: %d", resp.StatusCode())
	}
	return *resp.JSON201, nil
}

func (d *DataService) UploadURL(ctx context.Context, req UploadURLRequest) (internalapi.InternalSignedURL, error) {
	params := &internalapi.InternalUploadURLParams{}
	if req.Bucket != "" {
		params.Bucket = &req.Bucket
	}
	if req.FileName != "" {
		params.FileName = &req.FileName
	}
	if req.ExpiresIn > 0 {
		expires := int32(req.ExpiresIn)
		params.ExpiresIn = &expires
	}
	resp, err := d.gen.InternalUploadURLWithResponse(ctx, req.FileID, params)
	if err != nil {
		return internalapi.InternalSignedURL{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalSignedURL{}, fmt.Errorf("failed to get upload URL: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (d *DataService) UploadBulk(ctx context.Context, req internalapi.InternalUploadBulkRequest) (internalapi.InternalUploadBulkOutput, error) {
	resp, err := d.gen.InternalUploadBulkWithResponse(ctx, internalapi.InternalUploadBulkJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalUploadBulkOutput{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalUploadBulkOutput{}, fmt.Errorf("failed to upload bulk: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (d *DataService) DownloadURL(ctx context.Context, did string, expiresIn int, redirect bool) (internalapi.InternalSignedURL, error) {
	params := &internalapi.InternalDownloadParams{}
	if expiresIn > 0 {
		params.ExpiresIn = &expiresIn
	}
	if redirect {
		params.Redirect = &redirect
	}
	resp, err := d.gen.InternalDownloadWithResponse(ctx, did, params)
	if err != nil {
		return internalapi.InternalSignedURL{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalSignedURL{}, fmt.Errorf("failed to get download URL: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *DataService) multipartInitRequest(ctx context.Context, req internalapi.InternalMultipartInitRequest) (internalapi.InternalMultipartInitOutput, error) {
	resp, err := s.gen.InternalMultipartInitWithResponse(ctx, internalapi.InternalMultipartInitJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalMultipartInitOutput{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalMultipartInitOutput{}, fmt.Errorf("failed to init multipart: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (d *DataService) multipartUploadRequest(ctx context.Context, req internalapi.InternalMultipartUploadRequest) (internalapi.InternalMultipartUploadOutput, error) {
	resp, err := d.gen.InternalMultipartUploadWithResponse(ctx, internalapi.InternalMultipartUploadJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalMultipartUploadOutput{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalMultipartUploadOutput{}, fmt.Errorf("failed to upload part: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (d *DataService) multipartCompleteRequest(ctx context.Context, req internalapi.InternalMultipartCompleteRequest) error {
	resp, err := d.gen.InternalMultipartCompleteWithResponse(ctx, internalapi.InternalMultipartCompleteJSONRequestBody(req))
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("failed to complete multipart: %d", resp.StatusCode())
	}
	return nil
}

// --- transfer.WriteBackend interface support ---

func (d *DataService) GetWriter(ctx context.Context, guid string) (io.WriteCloser, error) {
	req := internalapi.InternalUploadBlankRequest{
		Guid: &guid,
	}
	_, err := d.UploadBlank(ctx, req)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("GetWriter not yet fully implemented for DataService")
}

func (d *DataService) Stat(ctx context.Context, guid string) (*transfer.ObjectMetadata, error) {
	if d.drs != nil {
		obj, err := d.drs.GetObject(ctx, guid)
		if err == nil {
			md := &transfer.ObjectMetadata{
				Size:     obj.Size,
				Provider: "drs",
			}
			if obj.AccessMethods != nil && len(*obj.AccessMethods) > 0 {
				md.AcceptRanges = true
			}
			return md, nil
		}
	}
	signedURL, err := d.ResolveDownloadURL(ctx, guid, "")
	if err != nil {
		return nil, err
	}
	return &transfer.ObjectMetadata{
		Provider:     "http",
		AcceptRanges: true,
		Size:         0,
		Checksums:    nil,
		MD5:          signedURL,
	}, nil
}

func (d *DataService) GetReader(ctx context.Context, guid string) (io.ReadCloser, error) {
	resp, err := d.Download(ctx, guid, nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (d *DataService) GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error) {
	var end *int64
	if length > 0 {
		e := offset + length - 1
		end = &e
	}
	resp, err := d.Download(ctx, guid, &offset, end)
	if err != nil {
		return nil, err
	}
	if offset > 0 && resp.StatusCode == http.StatusOK {
		resp.Body.Close()
		return nil, transfer.ErrRangeIgnored
	}
	return resp.Body, nil
}

// --- transfer.Downloader interface support ---

func (d *DataService) ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error) {
	resp, err := d.DownloadURL(ctx, guid, 0, false)
	if err != nil {
		return "", err
	}
	if resp.Url == nil {
		return "", fmt.Errorf("response missing URL")
	}
	return *resp.Url, nil
}

func (d *DataService) Download(ctx context.Context, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	return transfer.GenericDownload(ctx, d.requestor, signedURL, rangeStart, rangeEnd)
}

// --- transfer.Uploader interface support ---

func (d *DataService) ResolveUploadURL(ctx context.Context, guid, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	if strings.TrimSpace(bucket) == "" && strings.TrimSpace(guid) != "" {
		blank, err := d.UploadBlank(ctx, internalapi.InternalUploadBlankRequest{Guid: &guid})
		if err == nil && blank.Bucket != nil && strings.TrimSpace(*blank.Bucket) != "" {
			bucket = strings.TrimSpace(*blank.Bucket)
		}
	}
	resp, err := d.UploadURL(ctx, UploadURLRequest{
		FileID:   guid,
		FileName: filename,
		Bucket:   bucket,
	})
	if err != nil {
		return "", err
	}
	if resp.Url == nil {
		return "", fmt.Errorf("response missing URL")
	}
	d.rememberUploadBucket(guid, bucket)
	return *resp.Url, nil
}

func (d *DataService) Upload(ctx context.Context, url string, body io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	_, err := transfer.DoUpload(ctx, d.requestor, url, body, size)
	return err
}

func (d *DataService) UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	return transfer.DoUpload(ctx, d.requestor, url, body, size)
}

func (d *DataService) DeleteFile(ctx context.Context, guid string) (string, error) {
	resp, err := d.gen.InternalDeleteWithResponse(ctx, guid)
	if err != nil {
		return "", err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusNoContent {
		return "", fmt.Errorf("failed to delete file: %d", resp.StatusCode())
	}
	return guid, nil
}

func (d *DataService) Delete(ctx context.Context, guid string) error {
	_, err := d.DeleteFile(ctx, guid)
	return err
}

// --- transfer.Service interface support ---

func (d *DataService) Name() string { return "syfon-data-service" }

func (d *DataService) Logger() transfer.TransferLogger {
	return d.logger
}

func (d *DataService) Validate(ctx context.Context, bucket string) error {
	return nil
}

// --- transfer.MultipartURLSigner interface support ---

func (d *DataService) InitMultipartUpload(ctx context.Context, guid, filename, bucket string) (string, string, error) {
	req := internalapi.InternalMultipartInitRequest{
		Guid:     &guid,
		FileName: &filename,
		Bucket:   &bucket,
	}
	resp, err := d.multipartInitRequest(ctx, req)
	if err != nil {
		return "", "", err
	}
	uploadID := ""
	if resp.UploadId != nil {
		uploadID = *resp.UploadId
	}
	respGuid := ""
	if resp.Guid != nil {
		respGuid = *resp.Guid
	}
	return uploadID, respGuid, nil
}

func (d *DataService) GetMultipartUploadURL(ctx context.Context, key, uploadID string, partNum int32, bucket string) (string, error) {
	req := internalapi.InternalMultipartUploadRequest{
		Key:        key,
		UploadId:   uploadID,
		PartNumber: partNum,
		Bucket:     &bucket,
	}
	resp, err := d.multipartUploadRequest(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.PresignedUrl == nil {
		return "", fmt.Errorf("response missing presigned URL")
	}
	return *resp.PresignedUrl, nil
}

func (d *DataService) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error {
	return d.multipartCompleteRequest(ctx, internalapi.InternalMultipartCompleteRequest{
		Key:      key,
		UploadId: uploadID,
		Bucket:   &bucket,
		Parts:    parts,
	})
}

func (d *DataService) MultipartInit(ctx context.Context, guid string) (string, error) {
	uploadID, _, err := d.InitMultipartUpload(ctx, guid, "", "")
	return uploadID, err
}

func (d *DataService) MultipartPart(ctx context.Context, guid string, uploadID string, partNum int, body io.Reader) (string, error) {
	url, err := d.GetMultipartUploadURL(ctx, guid, uploadID, int32(partNum), "")
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return d.UploadPart(ctx, url, bytes.NewReader(data), int64(len(data)))
}

func (d *DataService) MultipartComplete(ctx context.Context, guid string, uploadID string, parts []transfer.MultipartPart) error {
	reqParts := make([]internalapi.InternalMultipartPart, 0, len(parts))
	for _, p := range parts {
		reqParts = append(reqParts, internalapi.InternalMultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}
	return d.multipartCompleteRequest(ctx, internalapi.InternalMultipartCompleteRequest{
		Key:      guid,
		UploadId: uploadID,
		Parts:    reqParts,
	})
}

func (d *DataService) CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error) {
	if strings.TrimSpace(bucketHint) == "" {
		bucketHint = d.uploadBucket(fallbackDID)
	}
	parsed, err := url.Parse(strings.TrimSpace(signedURL))
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}
	originalParsed := *parsed
	parsed.RawQuery = ""
	parsed.Fragment = ""

	switch strings.ToLower(parsed.Scheme) {
	case "file":
		return parsed.String(), nil
	case "http", "https":
		if b, k, ok := parseGCSJSONUploadURL(&originalParsed); ok {
			return "s3://" + b + "/" + k, nil
		}
		if b, k, ok := parseAzureBlobSignedURL(&originalParsed); ok {
			return "s3://" + b + "/" + k, nil
		}

		bucketHint = strings.TrimSpace(bucketHint)

		key := strings.Trim(strings.TrimSpace(parsed.Path), "/")

		// If bucketHint is empty, try to infer it from the first segment of the path (Path-Style)
		if bucketHint == "" {
			parts := strings.Split(key, "/")
			if len(parts) > 1 {
				bucketHint = parts[0]
				key = strings.Join(parts[1:], "/")
			}
		}

		if bucketHint == "" {
			return "", fmt.Errorf("unable to determine bucket context from URL: %s", signedURL)
		}

		// If the path starts with /bucket/, strip it to get the key.
		if strings.HasPrefix(key, bucketHint+"/") {
			key = strings.TrimPrefix(key, bucketHint+"/")
		}

		// Use s3:// as the standard internal representation for all HTTP-signed cloud storage (MinIO/S3/GCS)
		// unless we have specific knowledge to do otherwise.
		if key == "" {
			key = strings.TrimSpace(fallbackDID)
		}
		if key == "" {
			return "", fmt.Errorf("unable to derive object key from upload URL")
		}
		return "s3://" + bucketHint + "/" + key, nil
	default:
		if parsed.Scheme != "" && parsed.Host != "" {
			return parsed.String(), nil
		}
		return "s3://" + bucketHint + "/" + fallbackDID, nil
	}
}

func (d *DataService) rememberUploadBucket(guid, bucket string) {
	guid = strings.TrimSpace(guid)
	bucket = strings.TrimSpace(bucket)
	if guid == "" || bucket == "" {
		return
	}
	d.uploadMu.Lock()
	defer d.uploadMu.Unlock()
	if d.uploads == nil {
		d.uploads = map[string]string{}
	}
	d.uploads[guid] = bucket
}

func (d *DataService) uploadBucket(guid string) string {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return ""
	}
	d.uploadMu.RLock()
	defer d.uploadMu.RUnlock()
	return strings.TrimSpace(d.uploads[guid])
}

func parseGCSJSONUploadURL(parsed *url.URL) (bucket string, key string, ok bool) {
	if parsed == nil {
		return "", "", false
	}
	q := parsed.Query()
	if strings.TrimSpace(q.Get("uploadType")) != "media" {
		return "", "", false
	}
	key = strings.Trim(strings.TrimSpace(q.Get("name")), "/")
	if key == "" {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(parsed.Path), "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "b" {
			bucket = strings.TrimSpace(parts[i+1])
			break
		}
	}
	if bucket == "" {
		return "", "", false
	}
	return bucket, key, true
}

func parseAzureBlobSignedURL(parsed *url.URL) (bucket string, key string, ok bool) {
	if parsed == nil {
		return "", "", false
	}
	q := parsed.Query()
	if strings.TrimSpace(q.Get("sig")) == "" || !strings.EqualFold(strings.TrimSpace(q.Get("sr")), "b") {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(parsed.Path), "/"), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if strings.Contains(host, ".blob.") {
		bucket = strings.TrimSpace(parts[0])
		key = strings.Join(parts[1:], "/")
	} else {
		// Azurite path shape: /<account>/<container>/<key...>
		if len(parts) < 3 {
			return "", "", false
		}
		bucket = strings.TrimSpace(parts[1])
		key = strings.Join(parts[2:], "/")
	}
	bucket = strings.Trim(bucket, "/")
	key = strings.Trim(key, "/")
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}
