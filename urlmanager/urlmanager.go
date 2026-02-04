package urlmanager

import "context"

// SignOptions contains optional parameters for signing.
type SignOptions struct {
	ExpiresIn int // in seconds
}

// MultipartPart represents a part in a multipart upload.
type MultipartPart struct {
	PartNumber int32
	ETag       string
}

// UrlManager is responsible for signing URLs for resource access.
type UrlManager interface {
	// SignURL signs a URL for the given resource (Download).
	SignURL(ctx context.Context, accessId string, url string, opts SignOptions) (string, error)

	// SignUploadURL signs a URL for uploading a resource.
	SignUploadURL(ctx context.Context, accessId string, url string, opts SignOptions) (string, error)

	// Multipart Support
	InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error)
	SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []MultipartPart) error
}
