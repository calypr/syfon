package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/xfer"
)

type DataService struct {
	requestor request.Requester
	logger    *logs.Gen3Logger
	drs       *DRSService
}

func NewDataService(r request.Requester, l *logs.Gen3Logger, drs *DRSService) *DataService {
	return &DataService{
		requestor: r,
		logger:    l,
		drs:       drs,
	}
}

func (d *DataService) UploadBlank(ctx context.Context, req UploadBlankRequest) (UploadBlankResponse, error) {
	var out UploadBlankResponse
	err := d.requestor.Do(ctx, http.MethodPost, common.DataUploadEndpoint, req, &out)
	return out, err
}

func (d *DataService) UploadURL(ctx context.Context, req UploadURLRequest) (SignedURL, error) {
	q := url.Values{}
	if req.Bucket != "" {
		q.Set(common.QueryParamBucket, req.Bucket)
	}
	if req.FileName != "" {
		q.Set(common.QueryParamFileName, req.FileName)
	}
	if req.ExpiresIn > 0 {
		q.Set(common.QueryParamExpiresIn, strconv.Itoa(req.ExpiresIn))
	}
	var out SignedURL
	err := d.requestor.Do(ctx, http.MethodGet, fmt.Sprintf(common.DataRecordEndpointTemplate, url.PathEscape(req.FileID)), nil, &out, request.WithQueryValues(q))
	return out, err
}

func (d *DataService) UploadBulk(ctx context.Context, req UploadBulkRequest) (UploadBulkResponse, error) {
	var out UploadBulkResponse
	err := d.requestor.Do(ctx, http.MethodPost, common.DataUploadBulkEndpoint, req, &out)
	return out, err
}

func (d *DataService) DownloadURL(ctx context.Context, did string, expiresIn int, redirect bool) (SignedURL, error) {
	q := url.Values{}
	if expiresIn > 0 {
		q.Set(common.QueryParamExpiresIn, strconv.Itoa(expiresIn))
	}
	if redirect {
		q.Set(common.QueryParamRedirect, "true")
	}
	var out SignedURL
	err := d.requestor.Do(ctx, http.MethodGet, fmt.Sprintf(common.DataDownloadRecordEndpointTemplate, url.PathEscape(did)), nil, &out, request.WithQueryValues(q))
	return out, err
}

func (d *DataService) MultipartInit(ctx context.Context, req MultipartInitRequest) (MultipartInitResponse, error) {
	var out MultipartInitResponse
	err := d.requestor.Do(ctx, http.MethodPost, common.DataMultipartInitEndpoint, req, &out)
	return out, err
}

func (d *DataService) MultipartUpload(ctx context.Context, req MultipartUploadRequest) (MultipartUploadResponse, error) {
	var out MultipartUploadResponse
	err := d.requestor.Do(ctx, http.MethodPost, common.DataMultipartUploadEndpoint, req, &out)
	return out, err
}

func (d *DataService) MultipartComplete(ctx context.Context, req MultipartCompleteRequest) error {
	return d.requestor.Do(ctx, http.MethodPost, common.DataMultipartCompleteEndpoint, req, nil)
}

// --- transfer.ObjectWriter interface support ---

func (d *DataService) GetWriter(ctx context.Context, guid string) (io.WriteCloser, error) {
	req := UploadBlankRequest{
		Guid: &guid,
	}
	_, err := d.UploadBlank(ctx, req)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("GetWriter not yet fully implemented for DataService")
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
	return xfer.GenericDownload(ctx, d.requestor, signedURL, rangeStart, rangeEnd)
}

// --- transfer.Uploader interface support ---

func (d *DataService) ResolveUploadURL(ctx context.Context, guid, filename string, metadata common.FileMetadata, bucket string) (string, error) {
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
	return *resp.Url, nil
}

func (d *DataService) Upload(ctx context.Context, url string, body io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	_, err := xfer.DoUpload(ctx, d.requestor, url, body, size)
	return err
}

func (d *DataService) UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	return xfer.DoUpload(ctx, d.requestor, url, body, size)
}

func (d *DataService) DeleteFile(ctx context.Context, guid string) (string, error) {
	// Not implemented in backend yet, but required by interface
	return "", fmt.Errorf("DeleteFile not yet implemented for DataService")
}

// --- transfer.Service interface support ---

func (d *DataService) Name() string { return "syfon-data-service" }

func (d *DataService) Logger() xfer.TransferLogger {
	return d.logger
}

// --- transfer.MultipartURLSigner interface support ---

func (d *DataService) InitMultipartUpload(ctx context.Context, guid, filename, bucket string) (string, string, error) {
	req := MultipartInitRequest{
		Guid:     &guid,
		FileName: &filename,
		Bucket:   &bucket,
	}
	resp, err := d.MultipartInit(ctx, req)
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
	req := MultipartUploadRequest{
		Key:        key,
		UploadId:   uploadID,
		PartNumber: partNum,
		Bucket:     &bucket,
	}
	resp, err := d.MultipartUpload(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.PresignedUrl == nil {
		return "", fmt.Errorf("response missing presigned URL")
	}
	return *resp.PresignedUrl, nil
}

func (d *DataService) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error {
	var apiParts []MultipartPart
	for _, p := range parts {
		apiParts = append(apiParts, MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}
	req := MultipartCompleteRequest{
		Key:      key,
		UploadId: uploadID,
		Bucket:   &bucket,
		Parts:    apiParts,
	}
	return d.MultipartComplete(ctx, req)
}

func (d *DataService) CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error) {
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
