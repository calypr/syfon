package upload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	drs "github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/vbauerster/mpb/v8"
)

// Upload is a unified catch-all function that automatically chooses between
// single-part and multipart upload based on file size.
func Upload(ctx context.Context, bk transfer.Uploader, req common.FileUploadRequestObject, showProgress bool) error {
	bk.Logger().DebugContext(ctx, "Processing Upload Request", "source", req.SourcePath)

	file, err := os.Open(req.SourcePath)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", req.SourcePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return fmt.Errorf("file is empty: %s", req.ObjectKey)
	}

	// Use Single-Part if file is smaller than 5GB (or your defined limit)
	if fileSize < 5*common.GB {
		bk.Logger().DebugContext(ctx, "performing single-part upload", "size", fileSize)
		return UploadSingle(ctx, bk, bk.Logger(), req, true)
	}
	bk.Logger().DebugContext(ctx, "performing multipart upload", "size", fileSize)
	return MultipartUpload(ctx, bk, req, file, showProgress)
}

// UploadSingleFile handles single-part upload with progress
func UploadSingleFile(ctx context.Context, bk transfer.Uploader, req common.FileUploadRequestObject, showProgress bool) error {
	logger := bk.Logger()
	file, err := os.Open(req.SourcePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, _ := file.Stat()
	if fi.Size() > common.FileSizeLimit {
		return fmt.Errorf("file exceeds 5GB limit")
	}

	// Generate request with progress bar
	var p *mpb.Progress
	if showProgress {
		p = mpb.New(mpb.WithOutput(os.Stdout))
	}

	// Populate PresignedURL and GUID if missing
	fur, err := generateUploadRequest(ctx, bk, req, file, p)
	if err != nil {
		return err
	}

	if fi.Size() < int64(common.FileSizeLimit) {
		return UploadSingle(ctx, bk, logger, fur, true)
	}
	return MultipartUpload(ctx, bk, fur, file, showProgress)
}

// RegisterAndUploadFile orchestrates registration with Indexd and uploading via Fence.
// It handles checking for existing records, upsert logic, checking if file is already downloadable, and performing the upload.
func RegisterAndUploadFile(ctx context.Context, dc drs.Client, bk transfer.Uploader, drsObject *drs.DRSObject, filePath string, bucketName string, upsert bool) (*drs.DRSObject, error) {
	logger := bk.Logger()
	res, err := dc.RegisterRecord(ctx, drsObject)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			if !upsert {
				logger.DebugContext(ctx, "record already exists", "id", drsObject.Id)
			} else {
				logger.DebugContext(ctx, "record already exists, recreating", "id", drsObject.Id)
				err = dc.DeleteRecord(ctx, drsObject.Id)
				if err != nil {
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

	// 2. Check if file is downloadable
	downloadable, err := isFileDownloadable(ctx, dc, drsObject.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to check if file is downloadable: %w", err)
	}

	if downloadable {
		logger.DebugContext(ctx, "file already downloadable, skipping upload", "id", drsObject.Id)
		if res != nil {
			return res, nil
		}
		return dc.GetObject(ctx, drsObject.Id)
	}

	// 3. Upload File
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
				candidate := parts[len(parts)-1]
				if candidate != "" {
					uploadFilename = candidate
				}
			}
			break
		}
	}

	req := common.FileUploadRequestObject{
		SourcePath: filePath,
		ObjectKey:  uploadFilename,
		GUID:       drsObject.Id,
		Bucket:     bucketName,
	}

	err = Upload(ctx, bk, req, false)
	if err != nil {
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
	if err != nil {
		return false, nil
	}

	if res.Url == "" {
		return false, nil
	}

	err = common.CanDownloadFile(res.Url)
	return err == nil, nil
}
