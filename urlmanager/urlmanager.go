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

// SignedURLManager signs download/upload URLs for a storage backend.
type SignedURLManager interface {
	// SignURL signs a URL for the given resource (Download).
	SignURL(ctx context.Context, accessId string, url string, opts SignOptions) (string, error)

	// SignUploadURL signs a URL for uploading a resource.
	SignUploadURL(ctx context.Context, accessId string, url string, opts SignOptions) (string, error)

	// SignDownloadPart signs a URL for downloading a specific byte range.
	SignDownloadPart(ctx context.Context, accessId string, url string, start int64, end int64, opts SignOptions) (string, error)
}

// MultipartManager handles multipart lifecycle for a storage backend.
type MultipartManager interface {
	InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error)
	SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []MultipartPart) error
}

// UrlManager composes signed URL operations and multipart operations.
type UrlManager interface {
	SignedURLManager
	MultipartManager
}
