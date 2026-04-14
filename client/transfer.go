package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	sylogs "github.com/calypr/syfon/client/pkg/logs"
	syxfer "github.com/calypr/syfon/client/xfer"
	sydownload "github.com/calypr/syfon/client/xfer/download"
	syupload "github.com/calypr/syfon/client/xfer/upload"
)

type UploadOptions struct {
	Bucket             string
	IncludeSubDirName  bool
	HasMetadata        bool
	Batch              bool
	NumParallel        int
	ManifestPath       string
	RetryFailedLogPath string
	ShowProgress       bool
	ForceMultipart     bool
	GUID               string
}

type DownloadOptions struct {
	DownloadPath   string
	FilenameFormat string
	Rename         bool
	NoPrompt       bool
	Protocol       string
	NumParallel    int
	SkipCompleted  bool
	ManifestPath   string
}

type uploadManifestEntry struct {
	GUID  string `json:"guid"`
	Title string `json:"title"`
}

type downloadManifestEntry struct {
	GUID string `json:"guid"`
}

// Upload handles local file uploads, manifest-driven uploads, and retrying failed uploads.
// It keeps transfer orchestration in the client package so callers only pass flags and paths.
func Upload(ctx context.Context, api drs.Client, sourcePath string, opts UploadOptions) error {
	uploader, ok := api.(syxfer.Uploader)
	if !ok {
		return fmt.Errorf("drs client does not implement xfer.Uploader")
	}
	logger := api.Logger()
	if logger == nil {
		logger = sylogs.NewGen3Logger(nil, "", "")
	}
	if strings.TrimSpace(opts.Bucket) == "" {
		opts.Bucket = strings.TrimSpace(api.GetBucketName())
	}
	if opts.NumParallel <= 0 {
		opts.NumParallel = 1
	}

	if strings.TrimSpace(opts.RetryFailedLogPath) != "" {
		return retryFailedUploadsFromFile(ctx, uploader, logger, opts.RetryFailedLogPath)
	}

	if strings.TrimSpace(opts.ManifestPath) != "" {
		return uploadFromManifest(ctx, uploader, logger, sourcePath, opts)
	}
	return uploadFromPath(ctx, uploader, logger, sourcePath, opts)
}

// Download handles single- and multi-file downloads and manifest expansion.
func Download(ctx context.Context, api drs.Client, guids []string, opts DownloadOptions) error {
	downloader, ok := api.(syxfer.Downloader)
	if !ok {
		return fmt.Errorf("drs client does not implement xfer.Downloader")
	}

	if strings.TrimSpace(opts.ManifestPath) != "" {
		manifestGuids, err := loadDownloadManifest(opts.ManifestPath)
		if err != nil {
			return err
		}
		guids = append(guids, manifestGuids...)
	}

	if len(guids) == 0 {
		return fmt.Errorf("no guids provided for download")
	}

	downloadPath := strings.TrimSpace(opts.DownloadPath)
	if downloadPath == "" {
		downloadPath = "."
	}

	filenameFormat := strings.TrimSpace(opts.FilenameFormat)
	if filenameFormat == "" {
		filenameFormat = "original"
	}

	if opts.NumParallel <= 0 {
		opts.NumParallel = 1
	}

	return sydownload.DownloadMultiple(
		ctx,
		api,
		downloader,
		guids,
		downloadPath,
		filenameFormat,
		opts.Rename,
		opts.NoPrompt,
		opts.Protocol,
		opts.NumParallel,
		opts.SkipCompleted,
	)
}

func uploadFromPath(ctx context.Context, bk syxfer.Uploader, logger syxfer.TransferLogger, sourcePath string, opts UploadOptions) error {
	absUploadPath, err := common.GetAbsolutePath(sourcePath)
	if err != nil {
		return fmt.Errorf("resolve upload path: %w", err)
	}
	filePaths, err := common.ParseFilePaths(absUploadPath, opts.HasMetadata)
	if err != nil {
		return fmt.Errorf("parse upload paths: %w", err)
	}
	if len(filePaths) == 0 {
		return fmt.Errorf("no files found under %s", absUploadPath)
	}

	if opts.Batch {
		syupload.BatchUpload(ctx, bk, logger, absUploadPath, filePaths, opts.NumParallel, opts.Bucket, opts.IncludeSubDirName, opts.HasMetadata)
		if len(logger.GetSucceededLogMap()) == 0 {
			if failed := logger.GetFailedLogMap(); len(failed) > 0 {
				syupload.RetryFailedUploads(ctx, bk, logger, failed)
			}
		}
		if failed := logger.GetFailedLogMap(); len(failed) > 0 {
			return fmt.Errorf("%d upload(s) failed", len(failed))
		}
		return nil
	}

	var uploadErr error
	for _, filePath := range filePaths {
		src, key, metadata, err := syupload.ProcessFilename(logger, absUploadPath, filePath, "", opts.IncludeSubDirName, opts.HasMetadata)
		if err != nil {
			logger.Failed(filePath, filepath.Base(filePath), common.FileMetadata{}, "", 0, false)
			if uploadErr == nil {
				uploadErr = err
			}
			continue
		}

		if err := uploadOne(ctx, bk, src, key, opts.GUID, opts.Bucket, metadata, opts.ShowProgress, opts.ForceMultipart); err != nil {
			logger.Error("Upload failed", "path", src, "error", err)
			if uploadErr == nil {
				uploadErr = err
			}
		}
	}

	if len(logger.GetSucceededLogMap()) == 0 {
		if failed := logger.GetFailedLogMap(); len(failed) > 0 {
			syupload.RetryFailedUploads(ctx, bk, logger, failed)
		}
	}
	return uploadErr
}

func uploadFromManifest(ctx context.Context, bk syxfer.Uploader, logger syxfer.TransferLogger, uploadPath string, opts UploadOptions) error {
	absUploadPath, err := common.GetAbsolutePath(uploadPath)
	if err != nil {
		return fmt.Errorf("resolve upload path: %w", err)
	}
	manifestBytes, err := os.ReadFile(opts.ManifestPath)
	if err != nil {
		return fmt.Errorf("read manifest %s: %w", opts.ManifestPath, err)
	}

	var objects []uploadManifestEntry
	if err := json.Unmarshal(manifestBytes, &objects); err != nil {
		return fmt.Errorf("parse manifest %s: %w", opts.ManifestPath, err)
	}

	var uploadErr error
	for _, obj := range objects {
		localFilePath := filepath.Join(absUploadPath, obj.Title)
		src, key, metadata, err := syupload.ProcessFilename(logger, absUploadPath, localFilePath, obj.GUID, opts.IncludeSubDirName, opts.HasMetadata)
		if err != nil {
			logger.Failed(localFilePath, filepath.Base(localFilePath), common.FileMetadata{}, obj.GUID, 0, false)
			if uploadErr == nil {
				uploadErr = err
			}
			continue
		}
		if err := uploadOne(ctx, bk, src, key, obj.GUID, opts.Bucket, metadata, opts.ShowProgress, opts.ForceMultipart); err != nil {
			logger.Error("Upload failed", "path", src, "guid", obj.GUID, "error", err)
			if uploadErr == nil {
				uploadErr = err
			}
		}
	}

	if len(logger.GetSucceededLogMap()) == 0 {
		if failed := logger.GetFailedLogMap(); len(failed) > 0 {
			syupload.RetryFailedUploads(ctx, bk, logger, failed)
		}
	}
	return uploadErr
}

func uploadOne(
	ctx context.Context,
	bk syxfer.Uploader,
	sourcePath, objectKey, guid, bucket string,
	metadata common.FileMetadata,
	showProgress bool,
	forceMultipart bool,
) error {
	if forceMultipart {
		file, err := os.Open(sourcePath)
		if err != nil {
			return fmt.Errorf("cannot open file %s: %w", sourcePath, err)
		}
		defer file.Close()
		return syupload.MultipartUpload(ctx, bk, sourcePath, objectKey, guid, bucket, metadata, file, showProgress)
	}
	return syupload.Upload(ctx, bk, sourcePath, objectKey, guid, bucket, metadata, showProgress)
}

func retryFailedUploadsFromFile(ctx context.Context, bk syxfer.Uploader, logger syxfer.TransferLogger, failedLogPath string) error {
	data, err := os.ReadFile(failedLogPath)
	if err != nil {
		return fmt.Errorf("read failed log %s: %w", failedLogPath, err)
	}
	var failedMap map[string]common.RetryObject
	if err := json.Unmarshal(data, &failedMap); err != nil {
		return fmt.Errorf("parse failed log %s: %w", failedLogPath, err)
	}
	syupload.RetryFailedUploads(ctx, bk, logger, failedMap)
	return nil
}

func loadDownloadManifest(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var objects []downloadManifestEntry
	if err := json.Unmarshal(data, &objects); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	guids := make([]string, 0, len(objects))
	for _, obj := range objects {
		if strings.TrimSpace(obj.GUID) != "" {
			guids = append(guids, obj.GUID)
		}
	}
	return guids, nil
}
