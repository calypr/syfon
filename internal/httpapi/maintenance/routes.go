package maintenance

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/maintenance/projectstorage"
	"github.com/calypr/syfon/internal/maintenance/scoperepair"
	"github.com/gofiber/fiber/v3"
)

const (
	RouteInspectObject                 = "/data/inspect"
	RouteInspectObjectBulk             = "/data/inspect/bulk"
	RouteInspectObjectBulkList         = "/data/inspect/bulk-list"
	RouteInspectProjectBucket          = "/data/inspect/project-bucket"
	RouteInspectProjectBucketInventory = "/data/inspect/project-bucket/inventory"
	RouteInspectProjectRecords         = "/data/inspect/project-records"
	RouteInspectProjectScopes          = "/data/inspect/project-scopes"
	RouteDeleteProjectBucketObjects    = "/data/inspect/project-bucket/delete"
	RouteProjectCleanup                = "/data/projects/:organization/:project_id"
	RouteRepairScopeAudit              = "/data/repair/project-scope/audit"
	RouteRepairScopeApply              = "/data/repair/project-scope/apply"
)

func RegisterRepairRoutes(router fiber.Router, service *scoperepair.Service) {
	router.Post(RouteRepairScopeAudit, handleInternalScopeRepairAuditFiber(service))
	router.Post(RouteRepairScopeApply, handleInternalScopeRepairApplyFiber(service))
}

func RegisterInspectionRoutes(router fiber.Router, projectStorageService *projectstorage.Service, bucketService *buckets.Service) {
	router.Post(RouteInspectObject, handleInternalInspectObjectFiber(projectStorageService))
	router.Post(RouteInspectObjectBulk, handleInternalInspectObjectBulkFiber(projectStorageService))
	router.Post(RouteInspectObjectBulkList, handleInternalInspectObjectBulkListFiber(projectStorageService))
	router.Post(RouteInspectProjectBucket, handleInternalInspectProjectBucketFiber(projectStorageService))
	router.Post(RouteInspectProjectBucketInventory, handleInternalInspectProjectBucketInventoryFiber(projectStorageService))
	router.Post(RouteInspectProjectRecords, handleInternalInspectProjectRecordsFiber(projectStorageService))
	router.Get(RouteInspectProjectScopes, handleInternalInspectProjectScopesFiber(bucketService))
	router.Post(RouteInspectProjectScopes, handleInternalInspectProjectScopesFiber(bucketService))
	router.Post(RouteDeleteProjectBucketObjects, handleInternalDeleteProjectBucketObjectsFiber(projectStorageService))
}

func RegisterProjectCleanupRoutes(router fiber.Router, service *projectstorage.Service) {
	router.Delete(RouteProjectCleanup, func(c fiber.Ctx) error {
		return handleInternalDeleteProjectFiber(c, service)
	})
}
