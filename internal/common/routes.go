package common

const (
	RouteInternalDownload          = "/data/download/{file_id}"
	RouteInternalDownloadPart      = "/data/download/{file_id}/part"
	RouteInternalUpload            = "/data/upload"
	RouteInternalUploadURL         = "/data/upload/{file_id}"
	RouteInternalUploadBulk        = "/data/upload/bulk"
	RouteInternalMultipartInit     = "/data/multipart/init"
	RouteInternalMultipartUpload   = "/data/multipart/upload"
	RouteInternalMultipartComplete = "/data/multipart/complete"
	RouteInternalBuckets           = "/data/buckets"
	RouteInternalBucketDetail      = "/data/buckets/{bucket}"
	RouteInternalBucketScopes      = "/data/buckets/{bucket}/scopes"

	RouteInternalIndex            = "/index"
	RouteInternalIndexDetail      = "/index/{id}"
	RouteInternalBulkHashes       = "/index/bulk/hashes"
	RouteInternalBulkDeleteHashes = "/index/bulk/delete"
	RouteInternalBulkSHA256       = "/index/bulk/sha256/validity"
	RouteInternalBulkCreate       = "/index/bulk"
	RouteInternalBulkDocs         = "/index/bulk/documents"
)
