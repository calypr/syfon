package urlmanager

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/calypr/syfon/internal/provider"
)

type multipartBackend interface {
	Init(ctx context.Context, bucketName string, key string) (string, error)
	SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error)
	Complete(ctx context.Context, bucketName string, key string, uploadID string, parts []MultipartPart) error
}

func (m *Manager) getMultipartBackend(ctx context.Context, bucketName string, p string) (multipartBackend, error) {
	item, err := m.getBucket(ctx, bucketName, p)
	if err != nil {
		return nil, err
	}
	switch p {
	case provider.S3:
		return &s3MultipartBackend{m: m, item: item}, nil
	case provider.GCS:
		return &gcsMultipartBackend{m: m, item: item}, nil
	case provider.Azure:
		return &azureMultipartBackend{m: m, item: item}, nil
	default:
		return nil, fmt.Errorf("unsupported multipart provider: %s", p)
	}
}

func normalizedMultipartParts(parts []MultipartPart) []MultipartPart {
	partList := append([]MultipartPart(nil), parts...)
	sort.Slice(partList, func(i, j int) bool {
		return partList[i].PartNumber < partList[j].PartNumber
	})
	return partList
}

func multipartPartObjectKey(key string, uploadID string, partNumber int32) string {
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	return path.Join(".syfon-multipart", strings.TrimSpace(uploadID), cleanKey, "parts", strconv.Itoa(int(partNumber)))
}
