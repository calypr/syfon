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
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/xfer"
)

type DataService struct {
	base *baseService
	drs  *DRSService
}

func (d *DataService) UploadBlank(ctx context.Context, req UploadBlankRequest) (UploadBlankResponse, error) {
	var out UploadBlankResponse
	rb, err := d.base.requestor.New("POST", "/data/upload").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) UploadURL(ctx context.Context, req UploadURLRequest) (SignedURL, error) {
	q := url.Values{}
	if req.Bucket != "" {
		q.Set("bucket", req.Bucket)
	}
	if req.FileName != "" {
		q.Set("file_name", req.FileName)
	}
	if req.ExpiresIn > 0 {
		q.Set("expires_in", strconv.Itoa(req.ExpiresIn))
	}
	var out SignedURL
	rb := d.base.requestor.New("GET", "/data/upload/"+url.PathEscape(req.FileID)).WithQueryValues(q)
	err := d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) UploadBulk(ctx context.Context, req UploadBulkRequest) (UploadBulkResponse, error) {
	var out UploadBulkResponse
	rb, err := d.base.requestor.New("POST", "/data/upload/bulk").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) DownloadURL(ctx context.Context, did string, expiresIn int, redirect bool) (SignedURL, error) {
	q := url.Values{}
	if expiresIn > 0 {
		q.Set("expires_in", strconv.Itoa(expiresIn))
	}
	if redirect {
		q.Set("redirect", "true")
	}
	var out SignedURL
	rb := d.base.requestor.New("GET", "/data/download/"+url.PathEscape(did)).WithQueryValues(q)
	err := d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) MultipartInit(ctx context.Context, req MultipartInitRequest) (MultipartInitResponse, error) {
	var out MultipartInitResponse
	rb, err := d.base.requestor.New("POST", "/data/multipart/init").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) MultipartUpload(ctx context.Context, req MultipartUploadRequest) (MultipartUploadResponse, error) {
	var out MultipartUploadResponse
	rb, err := d.base.requestor.New("POST", "/data/multipart/upload").WithJSONBody(req)
	if err != nil {
		return out, err
	}
	err = d.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (d *DataService) MultipartComplete(ctx context.Context, req MultipartCompleteRequest) error {
	rb, err := d.base.requestor.New("POST", "/data/multipart/complete").WithJSONBody(req)
	if err != nil {
		return err
	}
	return d.base.requestor.DoJSON(ctx, rb, nil)
}

// --- transfer.ObjectWriter interface support ---

func (d *DataService) GetWriter(ctx context.Context, guid string) (io.WriteCloser, error) {
	req := UploadBlankRequest{}
	req.SetGuid(guid)
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
	return resp.GetUrl(), nil
}

func (d *DataService) Download(ctx context.Context, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	return xfer.GenericDownload(ctx, d.base.requestor, signedURL, rangeStart, rangeEnd)
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
	return resp.GetUrl(), nil
}
 
func (d *DataService) Upload(ctx context.Context, url string, body io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	_, err := xfer.DoUpload(ctx, d.base.requestor, url, body, size)
	return err
}
 
func (d *DataService) UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()
	return xfer.DoUpload(ctx, d.base.requestor, url, body, size)
}

func (d *DataService) DeleteFile(ctx context.Context, guid string) (string, error) {
	// Not implemented in backend yet, but required by interface
	return "", fmt.Errorf("DeleteFile not yet implemented for DataService")
}

// --- transfer.Service interface support ---

func (d *DataService) Name() string { return "syfon-data-service" }

func (d *DataService) Logger() xfer.TransferLogger {
	if r, ok := d.base.requestor.(*request.Request); ok {
		return r.Logs
	}
	return logs.NewGen3Logger(nil, "", "")
}

// --- transfer.MultipartURLSigner interface support ---

func (d *DataService) InitMultipartUpload(ctx context.Context, guid, filename, bucket string) (string, string, error) {
	req := MultipartInitRequest{}
	req.SetGuid(guid)
	req.SetFileName(filename)
	req.SetBucket(bucket)
	resp, err := d.MultipartInit(ctx, req)
	if err != nil {
		return "", "", err
	}
	return resp.GetUploadId(), resp.GetGuid(), nil
}

func (d *DataService) GetMultipartUploadURL(ctx context.Context, key, uploadID string, partNum int32, bucket string) (string, error) {
	req := MultipartUploadRequest{}
	req.SetKey(key)
	req.SetUploadId(uploadID)
	req.SetPartNumber(partNum)
	req.SetBucket(bucket)
	resp, err := d.MultipartUpload(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.GetPresignedUrl(), nil
}

func (d *DataService) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error {
	var apiParts []MultipartPart
	for _, p := range parts {
		apiParts = append(apiParts, MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}
	req := MultipartCompleteRequest{}
	req.SetKey(key)
	req.SetUploadId(uploadID)
	req.SetBucket(bucket)
	req.SetParts(apiParts)
	return d.MultipartComplete(ctx, req)
}

func (d *DataService) CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(signedURL))
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""

	switch strings.ToLower(parsed.Scheme) {
	case "file":
		return parsed.String(), nil
	case "http", "https":
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
