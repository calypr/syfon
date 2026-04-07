package transfer

import (
	"context"
	"io"
	"net/http"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/pkg/logs"
)

// Service provides high-level identity and naming.
type Service interface {
	Name() string
	Logger() *logs.Gen3Logger
}

// ObjectMetadata carries provider-agnostic information about a storage target.
type ObjectMetadata struct {
	Size         int64
	Checksums    []hash.HashInfo
	MD5          string
	AcceptRanges bool
	Provider     string // e.g. aws, gcp, azure, fs, http
}

// ObjectReader provides metadata and single-stream read access.
type ObjectReader interface {
	Stat(ctx context.Context, guid string) (*ObjectMetadata, error)
	GetReader(ctx context.Context, guid string) (io.ReadCloser, error)
}

// RangeReader enables high-performance parallel downloads.
type RangeReader interface {
	ObjectReader
	GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error)
}

// ObjectWriter provides single-stream write access.
type ObjectWriter interface {
	GetWriter(ctx context.Context, guid string) (io.WriteCloser, error)
}

// MultipartWriter enables specialized multipart uploads (init/part/complete).
type MultipartWriter interface {
	ObjectWriter
	MultipartInit(ctx context.Context, guid string) (string, error)
	MultipartPart(ctx context.Context, guid string, uploadID string, partNum int, body io.Reader) (string, error)
	MultipartComplete(ctx context.Context, guid string, uploadID string, parts []hash.HashInfo) error
}

// ObjectDeleter handles cleanup.
type ObjectDeleter interface {
	Delete(ctx context.Context, guid string) error
}

// SignedURL contains a URL and any mandatory headers for access.
type SignedURL struct {
	URL     string
	Headers map[string]string
}

// PartSigner is a generic interface for components that can sign byte ranges.
// This decouples the transfer package from the higher-level DRS client.
type PartSigner interface {
	GetDownloadPartURL(ctx context.Context, id string, start, end int64) (*SignedURL, error)
	Logger() *logs.Gen3Logger
}

// Downloader is the signed URL resolution and byte download surface.
type Downloader interface {
	Service
	ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error)
	Download(ctx context.Context, fdr *common.FileDownloadResponseObject) (*http.Response, error)
}

// Uploader is the signed URL and multipart upload surface.
type Uploader interface {
	Service
	ResolveUploadURL(ctx context.Context, guid string, filename string, metadata common.FileMetadata, bucket string) (string, error)
	ResolveUploadURLs(ctx context.Context, requests []common.UploadURLResolveRequest) ([]common.UploadURLResolveResponse, error)
	InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (*common.MultipartUploadInit, error)
	GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error)
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []common.MultipartUploadPart, bucket string) error
	Upload(ctx context.Context, url string, body io.Reader, size int64) error
	UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error)
	DeleteFile(ctx context.Context, guid string) (string, error)
}

// Backend embeds capabilities. Adapters can satisfy all or some of these.
type Backend interface {
	Service
	RangeReader
	ObjectWriter
	MultipartWriter
	ObjectDeleter
}
