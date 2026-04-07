package client

import (
	"context"
	"net/url"
	"strconv"
)

type DataService struct {
	c *Client
}

func (d *DataService) UploadBlank(ctx context.Context, req UploadBlankRequest) (UploadBlankResponse, error) {
	var out UploadBlankResponse
	err := d.c.doJSON(ctx, "POST", "/data/upload", nil, req, &out)
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
	err := d.c.doJSON(ctx, "GET", "/data/upload/"+url.PathEscape(req.FileID), q, nil, &out)
	return out, err
}

func (d *DataService) UploadBulk(ctx context.Context, req UploadBulkRequest) (UploadBulkResponse, error) {
	var out UploadBulkResponse
	err := d.c.doJSON(ctx, "POST", "/data/upload/bulk", nil, req, &out)
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
	err := d.c.doJSON(ctx, "GET", "/data/download/"+url.PathEscape(did), q, nil, &out)
	return out, err
}

func (d *DataService) MultipartInit(ctx context.Context, req MultipartInitRequest) (MultipartInitResponse, error) {
	var out MultipartInitResponse
	err := d.c.doJSON(ctx, "POST", "/data/multipart/init", nil, req, &out)
	return out, err
}

func (d *DataService) MultipartUpload(ctx context.Context, req MultipartUploadRequest) (MultipartUploadResponse, error) {
	var out MultipartUploadResponse
	err := d.c.doJSON(ctx, "POST", "/data/multipart/upload", nil, req, &out)
	return out, err
}

func (d *DataService) MultipartComplete(ctx context.Context, req MultipartCompleteRequest) error {
	return d.c.doJSON(ctx, "POST", "/data/multipart/complete", nil, req, nil)
}

// Compatibility wrappers used by current CLI code.
func (c *Client) RequestUploadURL(ctx context.Context, guid string) (SignedURL, error) {
	req := UploadBlankRequest{}
	req.SetGuid(guid)
	out, err := c.Data().UploadBlank(ctx, req)
	if err != nil {
		return SignedURL{}, err
	}
	signed := SignedURL{}
	signed.SetUrl(out.GetUrl())
	return signed, nil
}

func (c *Client) GetDownloadURL(ctx context.Context, did string) (SignedURL, error) {
	return c.Data().DownloadURL(ctx, did, 0, false)
}
