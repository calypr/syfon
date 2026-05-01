package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/common"
	sylogs "github.com/calypr/syfon/client/logs"
	"github.com/calypr/syfon/client/transfer"
	sydownload "github.com/calypr/syfon/client/transfer/download"
	syupload "github.com/calypr/syfon/client/transfer/upload"
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
func Upload(ctx context.Context, bk transfer.MultipartBackend, sourcePath string, opts UploadOptions) error {
	if bk == nil {
		return fmt.Errorf("backend is required")
	}
	logger := bk.Logger()
	if logger == nil {
		logger = sylogs.NewGen3Logger(nil, "", "")
	}
	if strings.TrimSpace(opts.Bucket) == "" {
		return fmt.Errorf("bucket is required")
	}
	if opts.NumParallel <= 0 {
		opts.NumParallel = 1
	}

	if strings.TrimSpace(opts.RetryFailedLogPath) != "" {
		return retryFailedUploadsFromFile(ctx, bk, logger, opts.RetryFailedLogPath)
	}

	if strings.TrimSpace(opts.ManifestPath) != "" {
		return uploadFromManifest(ctx, bk, logger, sourcePath, opts)
	}
	return uploadFromPath(ctx, bk, logger, sourcePath, opts)
}

// Download handles single- and multi-file downloads and manifest expansion.
func Download(ctx context.Context, api *Client, bk transfer.ReadBackend, guids []string, opts DownloadOptions) error {
	if api == nil {
		return fmt.Errorf("client is required")
	}
	if bk == nil {
		return fmt.Errorf("backend is required")
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

	if opts.NumParallel <= 0 {
		opts.NumParallel = 1
	}

	return sydownload.DownloadMultiple(
		ctx,
		api.DRS(),
		bk,
		guids,
		downloadPath,
		opts.NumParallel,
		opts.SkipCompleted,
	)
}

func uploadFromPath(ctx context.Context, bk transfer.MultipartBackend, logger transfer.TransferLogger, sourcePath string, opts UploadOptions) error {
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

	// For Batch uploads, we'll implement a simple loop calling the engine uploader.
	// This replaces the complex syupload.BatchUpload.
	var uploadErr error
	for _, filePath := range filePaths {
		// Note: ProcessFilename logic still useful for calculating destination object keys.
		// We'll keep it in syupload/utils.go for now but updated to the new logger.
		src, key, metadata, err := syupload.ProcessFilename(logger, absUploadPath, filePath, "", opts.IncludeSubDirName, opts.HasMetadata)
		if err != nil {
			logger.Failed(filePath, filepath.Base(filePath), common.FileMetadata{}, "", 0, false)
			if uploadErr == nil {
				uploadErr = err
			}
			continue
		}

		if err := syupload.Upload(ctx, bk, src, key, opts.GUID, opts.Bucket, metadata, opts.ShowProgress, opts.ForceMultipart); err != nil {
			logger.Error("Upload failed", "path", src, "error", err)
			if uploadErr == nil {
				uploadErr = err
			}
		}
	}

	return uploadErr
}

func uploadFromManifest(ctx context.Context, bk transfer.MultipartBackend, logger transfer.TransferLogger, uploadPath string, opts UploadOptions) error {
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
		if err := syupload.Upload(ctx, bk, src, key, obj.GUID, opts.Bucket, metadata, opts.ShowProgress, opts.ForceMultipart); err != nil {
			logger.Error("Upload failed", "path", src, "guid", obj.GUID, "error", err)
			if uploadErr == nil {
				uploadErr = err
			}
		}
	}

	return uploadErr
}

func retryFailedUploadsFromFile(ctx context.Context, bk transfer.MultipartBackend, logger transfer.TransferLogger, failedLogPath string) error {
	data, err := os.ReadFile(failedLogPath)
	if err != nil {
		return fmt.Errorf("read failed log %s: %w", failedLogPath, err)
	}
	var failedMap map[string]common.RetryObject
	if err := json.Unmarshal(data, &failedMap); err != nil {
		return fmt.Errorf("parse failed log %s: %w", failedLogPath, err)
	}

	for _, ro := range failedMap {
		if err := syupload.Upload(ctx, bk, ro.SourcePath, ro.ObjectKey, ro.GUID, ro.Bucket, ro.FileMetadata, true, ro.Multipart); err != nil {
			logger.Error("Retry failed", "path", ro.SourcePath, "error", err)
		} else {
			logger.DeleteFromFailedLog(ro.SourcePath)
		}
	}
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
