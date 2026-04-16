package transfer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/hash"
	"github.com/calypr/syfon/client/logs"
)

var (
	// ErrRangeIgnored indicates a server ignored a Range header and sent the full body.
	ErrRangeIgnored = fmt.Errorf("server ignored range request and returned full body")
)

// TransferLogger is the minimal logging surface used by the transfer engines.
type TransferLogger interface {
	Slog() *slog.Logger
	Info(msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	Error(msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	Warn(msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	Debug(msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
	Printf(format string, v ...any)
	Println(v ...any)
	Failed(filePath, filename string, metadata common.FileMetadata, guid string, retryCount int, multipart bool)
	FailedContext(ctx context.Context, filePath, filename string, metadata common.FileMetadata, guid string, retryCount int, multipart bool)
	Succeeded(filePath, guid string)
	SucceededContext(ctx context.Context, filePath, guid string)
	GetSucceededLogMap() map[string]string
	GetFailedLogMap() map[string]common.RetryObject
	DeleteFromFailedLog(path string)
	Scoreboard() *logs.Scoreboard
}

// Service provides high-level identity and logging access.
type Service interface {
	Name() string
	Logger() TransferLogger
}

// ObjectMetadata carries provider-agnostic information about a storage target.
type ObjectMetadata struct {
	Size         int64
	Checksums    []hash.HashInfo
	MD5          string
	AcceptRanges bool
	Provider     string
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

// ObjectWriter provides write access.
type ObjectWriter interface {
	GetWriter(ctx context.Context, guid string) (io.WriteCloser, error)
	Upload(ctx context.Context, guid string, body io.Reader, size int64) error
}

// MultipartPart represents a single uploaded segment.
type MultipartPart struct {
	PartNumber int32
	ETag       string
}

// MultipartWriter enables specialized multipart uploads (init/part/complete).
type MultipartWriter interface {
	ObjectWriter
	MultipartInit(ctx context.Context, guid string) (string, error)
	MultipartPart(ctx context.Context, guid string, uploadID string, partNum int, body io.Reader) (string, error)
	MultipartComplete(ctx context.Context, guid string, uploadID string, parts []MultipartPart) error
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
type PartSigner interface {
	GetDownloadPartURL(ctx context.Context, id string, start, end int64) (*SignedURL, error)
	Logger() TransferLogger
}

// Provider resolves the bucket/key for a GUID.
type Provider interface {
	GetStorageLocation(ctx context.Context, guid string) (bucket, key string, err error)
}

// Resolver handles logical-to-physical mapping.
type Resolver interface {
	Resolve(ctx context.Context, id string) (*ResolvedObject, error)
}

// Backend embeds capabilities and includes bucket validation.
type Backend interface {
	Service
	RangeReader
	ObjectWriter
	MultipartWriter
	ObjectDeleter
	Validate(ctx context.Context, bucket string) error
}

// Downloader is the signed URL resolution and byte download surface.
type Downloader interface {
	Service
	ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error)
	Download(ctx context.Context, url string, rangeStart, rangeEnd *int64) (*http.Response, error)
}

// Uploader is the signed URL and multipart upload surface.
type Uploader interface {
	Service
	ResolveUploadURL(ctx context.Context, guid string, filename string, metadata common.FileMetadata, bucket string) (string, error)
	InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (uploadID string, key string, err error)
	GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error)
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error
	Upload(ctx context.Context, url string, body io.Reader, size int64) error
	UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error)
	DeleteFile(ctx context.Context, guid string) (string, error)
	CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error)
}

// --- Plan & Strategy Types (moved from xfer/plan.go) ---

// ResolvedObject is the outcome of the resolution layer.
type ResolvedObject struct {
	Id           string
	Name         string
	Size         int64
	Checksums    hash.HashInfo
	ProviderURL  string
	AccessMethod string
}

// TransferStrategy defines how the engine intends to move the data.
type TransferStrategy string

const (
	StrategySingleStream   TransferStrategy = "single_stream"
	StrategyParallelRanges TransferStrategy = "parallel_ranges"
	StrategyMultipart      TransferStrategy = "multipart"
)

// TransferPlan is the blueprint for a specific data movement.
type TransferPlan struct {
	Strategy       TransferStrategy
	TotalSize      int64
	Chunks         []Chunk
	CheckpointPath string
}

type Chunk struct {
	Index  int
	Offset int64
	Length int64
	Status ChunkStatus
}

type ChunkStatus string

const (
	ChunkPending   ChunkStatus = "pending"
	ChunkCompleted ChunkStatus = "completed"
	ChunkFailed    ChunkStatus = "failed"
)

// Planner defines how to analyze a resolved object into a transfer plan.
type Planner interface {
	PlanDownload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
	PlanUpload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
}
