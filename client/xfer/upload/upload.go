package upload

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
)

func Upload(ctx context.Context, bk xfer.Uploader, sourcePath, objectKey, guid, bucket string, metadata common.FileMetadata, showProgress bool) error {
	req := uploadRequest{
		sourcePath: sourcePath,
		objectKey:  objectKey,
		guid:       guid,
		bucket:     bucket,
		metadata:   metadata,
	}
	file, err := os.Open(req.sourcePath)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", req.sourcePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %w", err)
	}

	if stat.Size() == 0 {
		return fmt.Errorf("file is empty: %s", req.objectKey)
	}

	if stat.Size() < 5*common.GB {
		return uploadSingle(ctx, bk, req, file, showProgress)
	}
	return uploadMultipart(ctx, bk, req, file, showProgress)
}

func uploadSingle(ctx context.Context, bk xfer.Uploader, req uploadRequest, file *os.File, showProgress bool) error {
	var p *mpb.Progress
	if showProgress {
		p = mpb.New(mpb.WithOutput(os.Stdout))
	}

	fur, err := generateUploadRequest(ctx, bk, req, file, p)
	if err != nil {
		return err
	}

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	var reader io.Reader = file
	if progressCallback := common.GetProgress(ctx); progressCallback != nil {
		reader = newProgressReader(file, progressCallback, resolveUploadOID(fur.objectKey, fur.guid), fi.Size())
	}

	err = bk.Upload(ctx, fur.presignedURL, reader, fi.Size())
	return err
}

func uploadMultipart(ctx context.Context, bk xfer.Uploader, req uploadRequest, file *os.File, showProgress bool) error {
	return MultipartUpload(ctx, bk, req.sourcePath, req.objectKey, req.guid, req.bucket, req.metadata, file, showProgress)
}

// RegisterAndUploadFile orchestrates registration with DRS and upload.
func RegisterAndUploadFile(ctx context.Context, dc drs.Client, bk xfer.Uploader, drsObject *drs.DRSObject, filePath string, bucketName string, upsert bool) (*drs.DRSObject, error) {
	if drsObject == nil {
		return nil, fmt.Errorf("drsObject must be provided (containing at least checksums/size)")
	}

	res, err := dc.RegisterRecord(ctx, drsObject)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			if !upsert {
				bk.Logger().DebugContext(ctx, "record already exists", "id", drsObject.Id)
			} else {
				if err = dc.DeleteRecord(ctx, drsObject.Id); err != nil {
					return nil, fmt.Errorf("failed to delete existing record: %w", err)
				}
				res, err = dc.RegisterRecord(ctx, drsObject)
				if err != nil {
					return nil, fmt.Errorf("failed to re-register record: %w", err)
				}
			}
		} else {
			return nil, fmt.Errorf("error registering record: %w", err)
		}
	}

	downloadable, err := isFileDownloadable(ctx, dc, drsObject.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to check if file is downloadable: %w", err)
	}
	if downloadable {
		if res != nil {
			return res, nil
		}
		return dc.GetObject(ctx, drsObject.Id)
	}

	uploadFilename := filepath.Base(filePath)
	if res != nil && len(res.AccessMethods) > 0 {
		for _, am := range res.AccessMethods {
			if am.Type != "s3" && am.Type != "gs" {
				continue
			}
			if am.AccessUrl.Url == "" {
				continue
			}
			parts := strings.Split(am.AccessUrl.Url, "/")
			if len(parts) > 0 {
				if candidate := parts[len(parts)-1]; candidate != "" {
					uploadFilename = candidate
				}
			}
			break
		}
	}

	if err := Upload(ctx, bk, filePath, uploadFilename, drsObject.Id, bucketName, common.FileMetadata{}, false); err != nil {
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}
	if res != nil {
		return res, nil
	}
	return dc.GetObject(ctx, drsObject.Id)
}

func isFileDownloadable(ctx context.Context, dc drs.Client, did string) (bool, error) {
	obj, err := dc.GetObject(ctx, did)
	if err != nil {
		return false, err
	}
	if len(obj.AccessMethods) == 0 {
		return false, nil
	}
	accessType := obj.AccessMethods[0].Type
	res, err := dc.GetDownloadURL(ctx, did, accessType)
	if err != nil || res.Url == "" {
		return false, nil
	}
	return common.CanDownloadFile(res.Url) == nil, nil
}
