package common

const (
	RouteInternalDownload           = "/data/download/{file_id}"
	RouteInternalDownloadPart       = "/data/download/{file_id}/part"
	RouteInternalUpload             = "/data/upload"
	RouteInternalUploadURL          = "/data/upload/{file_id}"
	RouteInternalUploadBulk         = "/data/upload/bulk"
	RouteInternalMultipartInit      = "/data/multipart/init"
	RouteInternalMultipartUpload    = "/data/multipart/upload"
	RouteInternalMultipartComplete  = "/data/multipart/complete"
	RouteInternalInspectObject      = "/data/inspect"
	RouteInternalBuckets            = "/data/buckets"
	RouteInternalBucketDetail       = "/data/buckets/{bucket}"
	RouteInternalBucketScopes       = "/data/buckets/{bucket}/scopes"
	RouteInternalProjectCleanup     = "/data/projects/{organization}/{project_id}"
	RouteInternalRepairCleanupAudit = "/data/repair/storage-cleanup/audit"
	RouteInternalRepairCleanupApply = "/data/repair/storage-cleanup/apply"
	RouteInternalRepairProjectDiff  = "/data/repair/project-diff/audit"

	RouteInternalIndex                       = "/index"
	RouteInternalIndexDetail                 = "/index/{id}"
	RouteInternalIndexControlledAccessRemove = "/index/{id}/controlled-access/remove"
	RouteInternalBulkHashes                  = "/index/bulk/hashes"
	RouteInternalBulkDeleteHashes            = "/index/bulk/delete"
	RouteInternalBulkSHA256                  = "/index/bulk/sha256/validity"
	RouteInternalBulkCreate                  = "/index/bulk"
	RouteInternalBulkDocs                    = "/index/bulk/documents"
)
