package transfer

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type MultipartURLSigner interface {
	InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (*common.MultipartUploadInit, error)
	GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error)
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []common.MultipartUploadPart, bucket string) error
}

var failMultipartOnce atomic.Bool

func MultipartUploadManaged(
	ctx context.Context,
	resolver Resolver,
	req common.FileUploadRequestObject,
	file *os.File,
	chunkSize int64,
) error {
	signer, ok := resolver.(MultipartURLSigner)
	if !ok {
		return fmt.Errorf("multipart upload requires signer capabilities (init/upload/complete); resolver does not implement MultipartURLSigner")
	}
	requestor, ok := resolver.(request.RequestInterface)
	if !ok {
		return fmt.Errorf("multipart upload requires request interface for presigned part uploads; resolver does not implement request.RequestInterface")
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if chunkSize <= 0 {
		chunkSize = OptimalChunkSize(fileSize)
	}
	if chunkSize < common.MinMultipartChunkSize {
		chunkSize = common.MinMultipartChunkSize
	}

	initResp, err := signer.InitMultipartUpload(ctx, req.GUID, req.ObjectKey, req.Bucket)
	if err != nil {
		return fmt.Errorf("multipart init failed: %w", err)
	}
	uploadID := strings.TrimSpace(initResp.UploadID)
	if uploadID == "" {
		return fmt.Errorf("multipart init did not return uploadId")
	}

	totalParts := int((fileSize + chunkSize - 1) / chunkSize)
	parts := make([]common.MultipartUploadPart, 0, totalParts)
	for i := 0; i < totalParts; i++ {
		partNum := int32(i + 1)
		offset := int64(i) * chunkSize
		length := chunkSize
		if remain := fileSize - offset; remain < length {
			length = remain
		}
		if os.Getenv("DATA_CLIENT_TEST_FAIL_UPLOAD_PART_ONCE") == "1" && partNum > 1 && failMultipartOnce.CompareAndSwap(false, true) {
			return fmt.Errorf("simulated network interruption during multipart upload")
		}

		partURL, err := signer.GetMultipartUploadURL(ctx, req.ObjectKey, uploadID, partNum, req.Bucket)
		if err != nil {
			return fmt.Errorf("multipart part url failed (part=%d): %w", partNum, err)
		}
		reader := io.NewSectionReader(file, offset, length)
		etag, err := DoUpload(ctx, requestor, partURL, reader, length)
		if err != nil {
			return fmt.Errorf("multipart part upload failed (part=%d): %w", partNum, err)
		}
		parts = append(parts, common.MultipartUploadPart{PartNumber: partNum, ETag: etag})
	}

	if err := signer.CompleteMultipartUpload(ctx, req.ObjectKey, uploadID, parts, req.Bucket); err != nil {
		return fmt.Errorf("multipart complete failed: %w", err)
	}
	return nil
}
