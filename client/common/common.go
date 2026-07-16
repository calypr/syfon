package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

func IsCloudPresignedURL(raw string) bool {
	return strings.Contains(raw, "X-Amz-Signature") ||
		strings.Contains(raw, "X-Goog-Signature") ||
		strings.Contains(raw, "Signature=") ||
		strings.Contains(raw, "AWSAccessKeyId=") ||
		strings.Contains(raw, "Expires=")
}
