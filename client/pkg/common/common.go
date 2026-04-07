package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	return fmt.Errorf("failed to access file, HTTP status: %d", resp.StatusCode)
}

func IsCloudPresignedURL(raw string) bool {
	return strings.Contains(raw, "X-Amz-Signature") ||
		strings.Contains(raw, "X-Goog-Signature") ||
		strings.Contains(raw, "Signature=") ||
		strings.Contains(raw, "AWSAccessKeyId=") ||
		strings.Contains(raw, "Expires=")
}
