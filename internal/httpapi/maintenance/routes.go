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

type Dependencies struct {
	ProjectStorageService *projectstorage.Service
	BucketService         *buckets.Service
	ScopeRepairService    *scoperepair.Service
}

func RegisterRoutes(router fiber.Router, deps Dependencies) {
	router.Post(RouteInspectObject, handleInternalInspectObjectFiber(deps.ProjectStorageService))
	router.Post(RouteInspectObjectBulk, handleInternalInspectObjectBulkFiber(deps.ProjectStorageService))
	router.Post(RouteInspectObjectBulkList, handleInternalInspectObjectBulkListFiber(deps.ProjectStorageService))
	router.Post(RouteInspectProjectBucket, handleInternalInspectProjectBucketFiber(deps.ProjectStorageService))
	router.Post(RouteInspectProjectBucketInventory, handleInternalInspectProjectBucketInventoryFiber(deps.ProjectStorageService))
	router.Post(RouteInspectProjectRecords, handleInternalInspectProjectRecordsFiber(deps.ProjectStorageService))
	router.Get(RouteInspectProjectScopes, handleInternalInspectProjectScopesFiber(deps.BucketService))
	router.Post(RouteInspectProjectScopes, handleInternalInspectProjectScopesFiber(deps.BucketService))
	router.Post(RouteDeleteProjectBucketObjects, handleInternalDeleteProjectBucketObjectsFiber(deps.ProjectStorageService))
	router.Delete(RouteProjectCleanup, func(c fiber.Ctx) error {
		return handleInternalDeleteProjectFiber(c, deps.ProjectStorageService)
	})
	router.Post(RouteRepairScopeAudit, handleInternalScopeRepairAuditFiber(deps.ScopeRepairService))
	router.Post(RouteRepairScopeApply, handleInternalScopeRepairApplyFiber(deps.ScopeRepairService))
}
