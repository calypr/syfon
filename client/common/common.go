package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func ToJSONReader(payload any) (io.Reader, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		return nil, fmt.Errorf("failed to encode JSON payload: %w", err)
	}
	return &buf, nil
}

func ParseRootPath(filePath string) (string, error) {
	if filePath != "" && filePath[0] == '~' {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return homeDir + filePath[1:], nil
	}
	return filePath, nil
}

func GetAbsolutePath(filePath string) (string, error) {
	fullFilePath, err := ParseRootPath(filePath)
	if err != nil {
		return "", err
	}
	return filepath.Abs(fullFilePath)
}

func ParseFilePaths(filePath string, metadataEnabled bool) ([]string, error) {
	fullFilePath, err := GetAbsolutePath(filePath)
	if err != nil {
		return []string{}, err
	}
	initialPaths, err := filepath.Glob(fullFilePath)
	if err != nil {
		return []string{}, err
	}

	var errs []error
	var finalFilePaths []string
	for _, p := range cleanupHiddenFiles(initialPaths) {
		file, err := os.Open(p)
		if err != nil {
			errs = append(errs, fmt.Errorf("file open error for %s: %w", p, err))
			continue
		}

		func(filePath string, file *os.File) {
			defer file.Close()

			fi, statErr := file.Stat()
			if statErr != nil {
				errs = append(errs, fmt.Errorf("file stat error for %s: %w", filePath, statErr))
				return
			}
			if fi.IsDir() {
				err = filepath.Walk(filePath, func(path string, fileInfo os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					isHidden, err := IsHidden(path)
					if err != nil {
						return err
					}
					isMetadata := false
					if metadataEnabled {
						isMetadata = strings.HasSuffix(path, "_metadata.json")
					}
					if !fileInfo.IsDir() && !isHidden && !isMetadata {
						finalFilePaths = append(finalFilePaths, path)
					}
					return nil
				})
				if err != nil {
					errs = append(errs, fmt.Errorf("directory walk error for %s: %w", filePath, err))
				}
			} else {
				finalFilePaths = append(finalFilePaths, filePath)
			}
		}(p, file)
	}

	return finalFilePaths, errors.Join(errs...)
}

func cleanupHiddenFiles(filePaths []string) []string {
	i := 0
	for _, filePath := range filePaths {
		isHidden, err := IsHidden(filePath)
		if err != nil {
			log.Println("Error occurred when checking hidden files: " + err.Error())
			continue
		}

		if isHidden {
			log.Printf("File %s is a hidden file and will be skipped\n", filePath)
			continue
		}
		filePaths[i] = filePath
		i++
	}
	return filePaths[:i]
}

func CanDownloadFile(signedURL string) error {
	req, err := http.NewRequest("GET", signedURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error while sending the request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPartialContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return ResponseBodyError(resp, "failed to access file")
}

func IsCloudPresignedURL(raw string) bool {
	return strings.Contains(raw, "X-Amz-Signature") ||
		strings.Contains(raw, "X-Goog-Signature") ||
		strings.Contains(raw, "Signature=") ||
		strings.Contains(raw, "AWSAccessKeyId=") ||
		strings.Contains(raw, "Expires=")
}

func LoadFailedLog(path string) (map[string]RetryObject, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]RetryObject
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
