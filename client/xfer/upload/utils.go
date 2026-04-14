package upload

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
)

func SeparateSingleAndMultipartUploads(bk interface{ Logger() xfer.TransferLogger }, objects []uploadRequest) ([]uploadRequest, []uploadRequest) {
	fileSizeLimit := common.FileSizeLimit
	logger := bk.Logger()

	var singlepartObjects []uploadRequest
	var multipartObjects []uploadRequest

	for _, object := range objects {
		fi, err := os.Stat(object.sourcePath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Error("The file you specified does not exist locally", "path", object.sourcePath)
			} else {
				logger.Error("File stat error", "path", object.sourcePath, "error", err)
			}
			continue
		}
		if fi.IsDir() {
			continue
		}
		if fi.Size() > common.MultipartFileSizeLimit {
			logger.Warn("File exceeds max limit", "name", fi.Name(), "size", fi.Size())
			continue
		}
		if fi.Size() > int64(fileSizeLimit) {
			multipartObjects = append(multipartObjects, object)
		} else {
			singlepartObjects = append(singlepartObjects, object)
		}
	}
	return singlepartObjects, multipartObjects
}

// ProcessFilename returns an FileInfo object which has the information about the path and name to be used for upload of a file
func ProcessFilename(logger xfer.TransferLogger, uploadPath string, filePath string, objectId string, includeSubDirName bool, includeMetadata bool) (string, string, common.FileMetadata, error) {
	var err error
	filePath, err = common.GetAbsolutePath(filePath)
	if err != nil {
		return "", "", common.FileMetadata{}, err
	}

	filename := filepath.Base(filePath) // Default to base filename

	var metadata common.FileMetadata
	if includeSubDirName {
		absUploadPath, err := common.GetAbsolutePath(uploadPath)
		if err != nil {
			return "", "", common.FileMetadata{}, err
		}

		// Ensure absUploadPath is a directory path for relative calculation
		// Trim the optional wildcard if present
		uploadDir := strings.TrimSuffix(absUploadPath, common.PathSeparator+"*")
		fileInfo, err := os.Stat(uploadDir)
		if err != nil {
			return "", "", common.FileMetadata{}, err
		}
		if fileInfo.IsDir() {
			// Calculate the path of the file relative to the upload directory
			relPath, err := filepath.Rel(uploadDir, filePath)
			if err != nil {
				return "", "", common.FileMetadata{}, err
			}
			filename = relPath
		}
	}

	if includeMetadata {
		// The metadata path is the file name plus '_metadata.json'
		metadataFilePath := strings.TrimSuffix(filePath, filepath.Ext(filePath)) + "_metadata.json"
		var metadataFileBytes []byte
		if _, err := os.Stat(metadataFilePath); err == nil {
			metadataFileBytes, err = os.ReadFile(metadataFilePath)
			if err != nil {
				return "", "", common.FileMetadata{}, errors.New("Error reading metadata file " + metadataFilePath + ": " + err.Error())
			}
			err := json.Unmarshal(metadataFileBytes, &metadata)
			if err != nil {
				return "", "", common.FileMetadata{}, errors.New("Error parsing metadata file " + metadataFilePath + ": " + err.Error())
			}
		} else {
			// No metadata file was found for this file -- proceed, but warn the user.
			logger.Printf("WARNING: File metadata is enabled, but could not find the metadata file %v for file %v. Execute `data-client upload --help` for more info on file metadata.\n", metadataFilePath, filePath)
		}
	}
	return filePath, filename, metadata, nil
}

// FormatSize helps to parse a int64 size into string
func FormatSize(size int64) string {
	var unitSize int64
	switch {
	case size >= common.TB:
		unitSize = common.TB
	case size >= common.GB:
		unitSize = common.GB
	case size >= common.MB:
		unitSize = common.MB
	case size >= common.KB:
		unitSize = common.KB
	default:
		unitSize = common.B
	}

	var unitMap = map[int64]string{
		common.B:  "B",
		common.KB: "KB",
		common.MB: "MB",
		common.GB: "GB",
		common.TB: "TB",
	}

	return fmt.Sprintf("%.1f"+unitMap[unitSize], float64(size)/float64(unitSize))
}

// OptimalChunkSize returns a recommended chunk size for the given fileSize (in bytes).
// - <= 100 MB: return fileSize (use single PUT)
// - >100 MB and <= 1 GB: 10 MB
// - >1 GB and <= 10 GB: scaled between 25 MB and 128 MB
// - >10 GB and <= 100 GB: 256 MB
// - >100 GB: scaled between 512 MB and 1024 MB (1 GB)
// See:
// https://cloud.switch.ch/-/documentation/s3/multipart-uploads/#best-practices
func OptimalChunkSize(fileSize int64) int64 {
	if fileSize <= 0 {
		return 1 * common.MB
	}

	switch {
	case fileSize <= 100*common.MB:
		// Single PUT: return whole file size
		return fileSize

	case fileSize <= 1*common.GB:
		return 10 * common.MB

	case fileSize <= 10*common.GB:
		return scaleLinear(fileSize, 1*common.GB, 10*common.GB, 25*common.MB, 128*common.MB)

	case fileSize <= 100*common.GB:
		return 256 * common.MB

	default:
		// Scale for very large files; cap scaling at 1 TB for ratio purposes
		return scaleLinear(fileSize, 100*common.GB, 1000*common.GB, 512*common.MB, 1024*common.MB)
	}
}

// scaleLinear scales size in [minSize, maxSize] to chunk in [minChunk, maxChunk] (linear).
// Result is rounded down to nearest MB and clamped to [minChunk, maxChunk].
func scaleLinear(size, minSize, maxSize, minChunk, maxChunk int64) int64 {
	if size <= minSize {
		return minChunk
	}
	if size >= maxSize {
		return maxChunk
	}
	ratio := float64(size-minSize) / float64(maxSize-minSize)
	chunkF := float64(minChunk) + ratio*(float64(maxChunk-minChunk))
	// round down to nearest MB
	mb := int64(common.MB)
	chunk := int64(chunkF) / mb * mb
	if chunk < minChunk {
		return minChunk
	}
	if chunk > maxChunk {
		return maxChunk
	}
	return chunk
}
