package copyproject

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/common"
	clienthash "github.com/calypr/syfon/client/hash"
	"github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/client/transfer/engine"
	"github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/transferprogress"

	"github.com/spf13/cobra"
)

type recordClient interface {
	Data() *services.DataService
	DRS() *services.DRSService
}

func copyRecord(ctx context.Context, cmd *cobra.Command, sourceClient, targetClient recordClient, rec internalapi.InternalRecord, targetBucket, targetProvider, targetProjectPath string, dstResource string, current, total int, tempDir string) error {
	did := rec.Did
	fileName := ""
	if rec.Name != nil {
		fileName = *rec.Name
	}
	size := int64(0)
	if rec.Size != nil {
		size = *rec.Size
	}
	checksum := ""
	if rec.Hashes != nil {
		if sha256Val, ok := (*rec.Hashes)["sha256"]; ok {
			checksum = sha256Val
		}
	}

	fmt.Fprintf(cmd.OutOrStdout(), "[%d/%d] Copying %s (size: %d, name: %s)...\n", current, total, did, size, fileName)

	tempFile, err := os.CreateTemp(tempDir, "copy-project-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %s: %w", tempPath, err)
	}
	defer os.Remove(tempPath)

	progressName := fileName
	if strings.TrimSpace(progressName) == "" {
		progressName = filepath.Base(tempPath)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Downloading %s -> %s", did, tempPath)
	if size > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", common.FormatSize(size))
	}
	fmt.Fprintln(cmd.OutOrStdout())

	downloadProgress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(progressName), size)
	downloadProgress.Start()
	downloadCtx := transferprogress.WithProgress(ctx, did, downloadProgress)
	if err := engine.Download(downloadCtx, sourceClient.Data(), did, tempPath, engine.DownloadOptions{MultipartThreshold: 5 * common.GB, EphemeralDestination: true}); err != nil {
		downloadProgress.Abort()
		return fmt.Errorf("failed to download file %s: %w", did, err)
	}
	downloadProgress.Finish()

	expectedChecksum := ""
	if strings.TrimSpace(checksum) != "" {
		expectedChecksum = clienthash.NormalizeOid(checksum)
		if expectedChecksum == "" {
			return fmt.Errorf("%w: invalid source SHA-256 checksum", errorapi.ErrInvalidInput)
		}
	}
	checksum, err = sha256File(tempPath)
	if err != nil {
		return fmt.Errorf("failed to compute SHA-256 for downloaded file %s: %w", did, err)
	}
	if expectedChecksum != "" && checksum != expectedChecksum {
		return fmt.Errorf("%w: source SHA-256 mismatch: downloaded file has %s, record says %s", errorapi.ErrInvalidInput, checksum, expectedChecksum)
	}

	drsObj := &drsapi.DrsObject{
		Id:          did,
		Name:        &fileName,
		Description: rec.Description,
		Size:        size,
		Checksums: []drsapi.Checksum{
			{Type: "sha256", Checksum: checksum},
		},
		ControlledAccess: &[]string{dstResource},
	}

	uploadKey := preferredUploadKey(rec.AccessMethods, checksum, fileName, tempPath)
	targetObjectURL := scopedObjectURL(targetProjectPath, targetBucket, uploadKey, targetProvider)

	fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s -> %s", did, targetObjectURL)
	if size > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", common.FormatSize(size))
	}
	fmt.Fprintln(cmd.OutOrStdout())

	uploadProgress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(progressName), size)
	uploadProgress.Start()
	uploadCtx := transferprogress.WithProgress(ctx, did, uploadProgress)
	if _, err := upload.RegisterFile(uploadCtx, targetClient.Data(), targetClient.DRS(), drsObj, tempPath, targetBucket, targetObjectURL); err != nil {
		uploadProgress.Abort()
		return fmt.Errorf("failed to upload file %s to target bucket %q: %w", did, targetBucket, err)
	}
	uploadProgress.Finish()

	return nil
}

func sha256File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func preferredUploadKey(accessMethods *[]drsapi.AccessMethod, checksum, fileName, filePath string) string {
	if strings.TrimSpace(checksum) != "" {
		return strings.TrimSpace(checksum)
	}
	key := path.Base(filePath)
	if accessMethods != nil {
		for _, am := range *accessMethods {
			if am.AccessUrl == nil || strings.TrimSpace(am.AccessUrl.Url) == "" {
				continue
			}
			parts := strings.Split(strings.TrimSpace(am.AccessUrl.Url), "/")
			if candidate := strings.TrimSpace(parts[len(parts)-1]); candidate != "" {
				key = candidate
				break
			}
		}
	}
	if strings.TrimSpace(fileName) != "" && key == path.Base(filePath) {
		key = strings.TrimSpace(fileName)
	}
	return key
}
