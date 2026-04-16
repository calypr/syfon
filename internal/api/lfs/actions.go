package lfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/urlmanager"
)

func prepareDownloadActions(ctx context.Context, database core.LFSStore, uM urlmanager.UrlManager, oid string) (*lfsapi.BatchActions, *lfsapi.ObjectError) {
	obj, err := resolveObjectForOID(ctx, database, oid)
	if err != nil {
		return nil, dbErrToBatchError(err, ctx)
	}
	if len(obj.Authorizations) > 0 && !hasMethodAccess(ctx, "read", obj.Authorizations) {
		return nil, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	}

	var src string
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl != nil && strings.TrimSpace(am.AccessUrl.Url) != "" {
				src = am.AccessUrl.Url
				break
			}
		}
	}
	if src == "" {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "no object location available"}
	}
	signed, err := uM.SignURL(ctx, "", src, urlmanager.SignOptions{})
	if err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	usageObjectID := oid
	if strings.TrimSpace(obj.Id) != "" {
		usageObjectID = strings.TrimSpace(obj.Id)
	}
	if recErr := database.RecordFileDownload(ctx, usageObjectID); recErr != nil {
		// Just log metric failures, don't break the download flow.
	}
	action := lfsapi.Action{Href: signed}
	return &lfsapi.BatchActions{Download: &action}, nil
}

func prepareUploadActions(ctx context.Context, database core.LFSStore, uM urlmanager.UrlManager, oid string, reqSize int64, baseURL string) (*lfsapi.BatchActions, int64, *lfsapi.ObjectError) {
	existing, err := resolveObjectForOID(ctx, database, oid)
	if err == nil {
		if len(existing.Authorizations) > 0 && !hasMethodAccess(ctx, "read", existing.Authorizations) {
			return nil, existing.Size, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
		}
		return nil, existing.Size, nil
	}
	if !isNotFound(err) {
		return nil, reqSize, dbErrToBatchError(err, ctx)
	}

	targetResources := []string{"/data_file"}
	if !hasMethodAccess(ctx, "file_upload", targetResources) && !hasMethodAccess(ctx, "create", targetResources) {
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	}

	creds, credErr := database.ListS3Credentials(ctx)
	if credErr != nil || len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
		if credErr == nil {
			credErr = fmt.Errorf("no bucket configured")
		}
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(http.StatusInsufficientStorage), Message: credErr.Error()}
	}
	size := reqSize
	if size < 0 {
		size = 0
	}
	uploadURL := baseURL + "/info/lfs/objects/" + oid
	verifyURL := baseURL + "/info/lfs/verify"
	uploadAction := lfsapi.Action{Href: uploadURL}
	verifyAction := lfsapi.Action{Href: verifyURL}
	return &lfsapi.BatchActions{
		Upload: &uploadAction,
		Verify: &verifyAction,
	}, size, nil
}

func proxySinglePut(ctx context.Context, uM urlmanager.UrlManager, bucket, key string) error {
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, key)
	signedURL, err := uM.SignUploadURL(ctx, "", s3URL, urlmanager.SignOptions{})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func uploadPartToSignedURL(ctx context.Context, signedURL string, content []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(content))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("multipart part put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	etag = strings.Trim(etag, "\"")
	if etag == "" {
		return "", fmt.Errorf("multipart part upload missing etag")
	}
	return etag, nil
}

func s3KeyFromCandidateForBucket(candidate drs.DrsObjectCandidate, bucket string) (string, bool) {
	targetBucket := strings.TrimSpace(bucket)
	if targetBucket == "" {
		return "", false
	}
	if candidate.AccessMethods != nil {
		for _, am := range *candidate.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || !strings.EqualFold(u.Scheme, "s3") {
				continue
			}
			if strings.TrimSpace(u.Host) != targetBucket {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}

func s3KeyFromObjectForBucket(obj *core.InternalObject, bucket string) (string, bool) {
	if obj == nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	if targetBucket == "" {
		return "", false
	}
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || !strings.EqualFold(u.Scheme, "s3") {
				continue
			}
			if strings.TrimSpace(u.Host) != targetBucket {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}
