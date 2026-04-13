package upload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
)

// RegisterFile orchestrates the full registration and upload flow:
// 1. Build a DRS object from the local file (if not provided).
// 2. Register metadata with the DRS server via the provided drs.Client.
// 3. Upload the file content via the provided Backend.
func RegisterFile(ctx context.Context, bk UploadBackend, dc drs.Client, drsObject *drs.DRSObject, filePath string, bucketName string) (*drs.DRSObject, error) {
	// 1. Ensure we have a valid OID/metadata.
	// (Logic ported and generalized from git-drs/client/local/local_client.go)

	if drsObject == nil {
		return nil, fmt.Errorf("drsObject must be provided (containing at least checksums/size)")
	}

	// 2. Register with DRS server
	res, err := dc.RegisterRecord(ctx, drsObject)
	if err != nil {
		return nil, fmt.Errorf("failed to register record: %w", err)
	}
	drsObject = res

	// 3. Check if file is already downloadable (optional but good optimization)
	// (Skipping for now to prioritize core functionality, but can be added back)

	// 4. Determine upload filename/key
	uploadFilename := filepath.Base(filePath)
	if len(drsObject.AccessMethods) > 0 {
		for _, am := range drsObject.AccessMethods {
			if am.Type == "s3" || am.Type == "gs" {
				if am.AccessUrl.Url == "" {
					continue
				}
				parts := strings.Split(am.AccessUrl.Url, "/")
				if candidate := parts[len(parts)-1]; candidate != "" {
					uploadFilename = candidate
					break
				}
			}
		}
	}

	// 5. Perform Upload
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for upload: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	threshold := int64(5 * common.GB) // Default threshold
	if stat.Size() < threshold {
		uploadURL, err := bk.ResolveUploadURL(ctx, drsObject.Id, uploadFilename, common.FileMetadata{}, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get upload URL: %w", err)
		}
		if err := bk.Upload(ctx, uploadURL, file, stat.Size()); err != nil {
			return nil, fmt.Errorf("upload failed: %w", err)
		}
	} else {
		if err := MultipartUpload(ctx, bk, filePath, uploadFilename, drsObject.Id, bucketName, common.FileMetadata{}, file, false); err != nil {
			return nil, fmt.Errorf("multipart upload failed: %w", err)
		}
	}

	return drsObject, nil
}

type UploadBackend = xfer.Uploader
