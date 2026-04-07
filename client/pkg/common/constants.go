package common

import (
	"os"
	"time"
)

const (
	// B is bytes
	B int64 = 1
	// KB is kilobytes
	KB int64 = 1024 * B
	// MB is megabytes
	MB int64 = 1024 * KB
	// GB is gigabytes
	GB int64 = 1024 * MB
	// TB is terabytes
	TB int64 = 1024 * GB
)
const (
	// DefaultUseShepherd sets whether gen3client will attempt to use the Shepherd / Object Management API
	// endpoints if available.
	// The user can override this default using the `data-client configure` command.
	DefaultUseShepherd = false

	// DefaultMinShepherdVersion is the minimum version of Shepherd that the gen3client will use.
	// Before attempting to use Shepherd, the client will check for Shepherd's version, and if the version is
	// below this number the gen3client will instead warn the user and fall back to fence/indexd.
	// The user can override this default using the `data-client configure` command.
	DefaultMinShepherdVersion = "2.0.0"

	// ShepherdEndpoint is the endpoint postfix for SHEPHERD / the Object Management API
	ShepherdEndpoint = "/mds"

	// ShepherdVersionEndpoint is the endpoint used to check what version of Shepherd a commons has deployed
	ShepherdVersionEndpoint = "/mds/version"

	// IndexdIndexEndpoint is the endpoint postfix for INDEXD index
	IndexdIndexEndpoint = "/index"

	// FenceUserEndpoint is the endpoint postfix for FENCE user
	FenceUserEndpoint = "/user/user"

	// FenceDataEndpoint is the canonical endpoint prefix for upload/delete flows
	FenceDataEndpoint = "/data/upload"

	// FenceAccessTokenEndpoint is the endpoint postfix for FENCE access token
	FenceAccessTokenEndpoint = "/user/credentials/api/access_token"

	// FenceDataUploadEndpoint is the endpoint postfix for upload init/presigned-url
	FenceDataUploadEndpoint = FenceDataEndpoint

	// FenceDataDownloadEndpoint is the endpoint postfix for download presigned-url
	FenceDataDownloadEndpoint = "/data/download"
	
	// FenceDataDownloadPartEndpoint is the endpoint postfix for download part presigned-url
	FenceDataDownloadPartEndpoint = "/data/download/%s/part"

	// FenceDataMultipartInitEndpoint is the endpoint postfix for multipart init
	FenceDataMultipartInitEndpoint = "/data/multipart/init"

	// FenceDataMultipartUploadEndpoint is the endpoint postfix for multipart upload
	FenceDataMultipartUploadEndpoint = "/data/multipart/upload"

	// FenceDataMultipartCompleteEndpoint is the endpoint postfix for multipart complete
	FenceDataMultipartCompleteEndpoint = "/data/multipart/complete"

	// PathSeparator is os dependent path separator char
	PathSeparator = string(os.PathSeparator)

	// DefaultTimeout is used to set timeout value for http client
	DefaultTimeout = 120 * time.Second

	HeaderContentType   = "Content-Type"
	MIMEApplicationJSON = "application/json"

	// FileSizeLimit is the maximum single file size for non-multipart upload (5GB)
	FileSizeLimit = 5 * GB

	// MultipartFileSizeLimit is the maximum single file size for multipart upload (5TB)
	MultipartFileSizeLimit = 5 * TB
	MinMultipartChunkSize  = 10 * MB

	// MaxRetryCount is the maximum retry number per record
	MaxRetryCount = 5
	MaxWaitTime   = 300

	MaxMultipartParts    = 10000
	MaxConcurrentUploads = 10
	MaxRetries           = 5

	OnProgressThreshold = 1 * MB
)

var (
	// MinChunkSize is configurable via git config and initialized in init()
	MinChunkSize = 10 * MB
)
