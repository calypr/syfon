package internaldrs

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/faults"
	apimiddleware "github.com/calypr/syfon/internal/httpapi/middleware"
	"github.com/calypr/syfon/internal/maintenance/projectstorage"
	"github.com/calypr/syfon/internal/storage/address"
)

func registerInternalBucketRoutes(router fiber.Router, om *core.ObjectManager, bucketService *buckets.Service, projectServices ...*projectstorage.Service) {
	router.Get(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalBucketsFiber(c, bucketService) })
	router.Put(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalPutBucketFiber(c, bucketService) })
	router.Delete(routeutil.FiberPath(common.RouteInternalBucketDetail), func(c fiber.Ctx) error { return handleInternalDeleteBucketFiber(c, bucketService) })
	router.Get(routeutil.FiberPath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalListBucketScopesFiber(c, bucketService) })
	router.Post(routeutil.FiberPath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalCreateBucketScopeFiber(c, bucketService) })
	router.Delete(routeutil.FiberPath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalDeleteBucketScopeFiber(c, bucketService) })
	if len(projectServices) > 0 && projectServices[0] != nil {
		router.Delete(routeutil.FiberPath(common.RouteInternalProjectCleanup), func(c fiber.Ctx) error { return handleInternalDeleteProjectFiber(c, projectServices[0]) })
	}
}

func handleInternalBucketsFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	if apimiddleware.MissingGen3AuthHeader(c.Context()) {
		return apiutil.HandleError(c, faults.ErrUnauthorized)
	}
	visible, err := bucketService.ListVisibleBuckets(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	resp := bucketapi.BucketsResponse{S3BUCKETS: map[string]bucketapi.BucketMetadata{}}
	for _, entry := range visible {
		cred := entry.Credential
		meta := bucketapi.BucketMetadata{
			Bucket:      common.Ptr(cred.Bucket),
			EndpointUrl: common.Ptr(cred.Endpoint),
			Provider:    common.Ptr(cred.Provider),
			Region:      common.Ptr(cred.Region),
		}
		if len(entry.Programs) > 0 {
			programs := append([]string(nil), entry.Programs...)
			meta.Programs = &programs
		}
		resp.S3BUCKETS[cred.Bucket] = meta
	}
	return c.JSON(resp)
}

func handleInternalPutBucketFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	var req bucketapi.PutBucketRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
	}

	rawProvider := strings.TrimSpace(common.StringVal(req.Provider))
	bucketProvider, err := address.ParseBucketProvider(rawProvider)
	if err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, "provider must be one of: s3, gcs, azure")
	}
	req.Provider = common.Ptr(bucketProvider)

	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	region := strings.TrimSpace(common.StringVal(req.Region))
	accessKey := strings.TrimSpace(common.StringVal(req.AccessKey))
	secretKey := strings.TrimSpace(common.StringVal(req.SecretKey))
	endpoint := strings.TrimSpace(common.StringVal(req.Endpoint))
	if req.Bucket == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "bucket is required")
	}
	if req.Organization == "" && req.ProjectId != "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "organization is required when project_id is set")
	}
	if err := authorizeBucketScopeWrite(c.Context(), req.Organization, req.ProjectId, "create", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}

	prefix, err := address.NormalizeStoragePath(readOptionalPath(req.Path), req.Bucket)
	if err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, err.Error())
	}

	credentialID := ""
	var existingCred *buckets.Credential
	var credErr error
	existingCred, credErr = bucketService.GetS3Credential(c.Context(), req.Bucket)
	if credErr != nil && !isCredentialNotFoundError(credErr) {
		return apiutil.HandleError(c, credErr)
	}
	if credErr == nil && existingCred != nil {
		credentialID = existingCred.CredentialID
	}
	if credentialID == "" {
		credentialID = buckets.DeriveCredentialID(req.Bucket, bucketProvider, region, endpoint, accessKey)
	}
	if existingCred == nil {
		existingCred, credErr = bucketService.GetS3Credential(c.Context(), credentialID)
	}
	hasExistingCred := credErr == nil && existingCred != nil
	if hasExistingCred && rawProvider == "" {
		bucketProvider = existingCred.Provider
		req.Provider = common.Ptr(bucketProvider)
	}
	scopeOnly := hasExistingCred &&
		accessKey == "" &&
		secretKey == "" &&
		endpoint == "" &&
		region == "" &&
		rawProvider == "" &&
		req.Organization != ""

	if !hasExistingCred && bucketProvider == address.S3Provider &&
		(accessKey == "" || secretKey == "") {
		return apiutil.Reject(c, fiber.StatusBadRequest, "access_key and secret_key are required for new s3 credentials")
	}

	if req.Organization != "" {
		if err := bucketService.CreateBucketScope(c.Context(), &buckets.Scope{
			Organization: req.Organization,
			ProjectID:    req.ProjectId,
			CredentialID: credentialID,
			Bucket:       req.Bucket,
			PathPrefix:   prefix,
		}); err != nil {
			return apiutil.HandleError(c, err)
		}
	}
	if scopeOnly {
		return c.SendStatus(fiber.StatusCreated)
	}

	if hasExistingCred {
		if region == "" {
			region = existingCred.Region
		}
		if accessKey == "" {
			accessKey = existingCred.AccessKey
		}
		if secretKey == "" {
			secretKey = existingCred.SecretKey
		}
		if endpoint == "" {
			endpoint = existingCred.Endpoint
		}
	}
	if err := address.ValidateBucketNameWithEndpoint(bucketProvider, req.Bucket, endpoint); err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, err.Error())
	}

	cred := &buckets.Credential{
		CredentialID: credentialID,
		Bucket:       req.Bucket,
		Provider:     bucketProvider,
		Region:       region,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Endpoint:     endpoint,
	}
	if bucketProvider == address.S3Provider && (strings.TrimSpace(cred.AccessKey) == "" || strings.TrimSpace(cred.SecretKey) == "") {
		return apiutil.Reject(c, fiber.StatusBadRequest, "access_key and secret_key are required for s3 credentials")
	}
	if err := bucketService.SaveS3Credential(c.Context(), cred); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusCreated)
}

func isCredentialNotFoundError(err error) bool {
	return err != nil && strings.EqualFold(strings.TrimSpace(err.Error()), "credential not found")
}

func handleInternalDeleteBucketFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	credentialID := strings.TrimSpace(c.Params("bucket"))
	if credentialID == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "bucket name is required")
	}
	if err := authorizeBucketDelete(c.Context(), bucketService, credentialID); err != nil {
		return apiutil.HandleError(c, err)
	}
	if err := bucketService.DeleteS3Credential(c.Context(), credentialID); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func handleInternalCreateBucketScopeFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	routeCredentialID := strings.TrimSpace(c.Params("bucket"))
	if routeCredentialID == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "credential id is required")
	}
	cred, err := bucketService.GetS3Credential(c.Context(), routeCredentialID)
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	var req bucketapi.AddBucketScopeRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
	}
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Organization == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "organization is required")
	}
	if err := authorizeBucketScopeWrite(c.Context(), req.Organization, req.ProjectId, "create", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}

	prefix, err := address.NormalizeStoragePath(readOptionalPath(req.Path), cred.Bucket)
	if err != nil {
		return apiutil.Reject(c, fiber.StatusBadRequest, err.Error())
	}
	if err := bucketService.CreateBucketScope(c.Context(), &buckets.Scope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		CredentialID: cred.CredentialID,
		Bucket:       cred.Bucket,
		PathPrefix:   prefix,
	}); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusCreated)
}

func handleInternalDeleteBucketScopeFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	routeCredentialID := strings.TrimSpace(c.Params("bucket"))
	if routeCredentialID == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "credential id is required")
	}
	hasPathQuery := c.Request().URI().QueryArgs().Has("path")
	organization := strings.TrimSpace(c.Query("organization"))
	scopePath := strings.TrimSpace(c.Query("path"))
	projectID := strings.TrimSpace(c.Query("project_id"))
	if organization == "" || !hasPathQuery {
		return apiutil.Reject(c, fiber.StatusBadRequest, "organization and path are required")
	}
	if err := authorizeBucketScopeWrite(c.Context(), organization, projectID, "delete", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}
	pathPrefix := ""
	if scopePath != "" {
		cred, err := bucketService.GetS3Credential(c.Context(), routeCredentialID)
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		pathPrefix, err = address.NormalizeStoragePath(scopePath, cred.Bucket)
		if err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, err.Error())
		}
	}
	scopes, err := bucketService.ListBucketScopes(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	matchCount := 0
	for _, scope := range scopes {
		if !(strings.EqualFold(scope.Bucket, routeCredentialID) || strings.EqualFold(scope.CredentialID, routeCredentialID)) {
			continue
		}
		if strings.TrimSpace(scope.Organization) != organization {
			continue
		}
		if strings.TrimSpace(scope.ProjectID) != projectID {
			continue
		}
		if strings.Trim(strings.TrimSpace(scope.PathPrefix), "/") != pathPrefix {
			continue
		}
		matchCount++
	}
	if matchCount == 0 {
		return apiutil.Reject(c, fiber.StatusNotFound, "bucket scope not found")
	}
	if matchCount > 1 {
		return apiutil.Reject(c, fiber.StatusConflict, "bucket scope delete matched multiple rows")
	}
	if err := bucketService.DeleteBucketScope(c.Context(), organization, projectID, routeCredentialID, pathPrefix); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func handleInternalListBucketScopesFiber(c fiber.Ctx, bucketService *buckets.Service) error {
	if apimiddleware.MissingGen3AuthHeader(c.Context()) {
		return apiutil.HandleError(c, faults.ErrUnauthorized)
	}
	routeCredentialID := strings.TrimSpace(c.Params("bucket"))
	if routeCredentialID == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "credential id is required")
	}

	scopes, err := bucketService.ListBucketScopes(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	result := []bucketapi.BucketScopeResponse{}
	for _, scope := range scopes {
		if strings.EqualFold(scope.Bucket, routeCredentialID) || strings.EqualFold(scope.CredentialID, routeCredentialID) {
			if !bucketScopeAllowed(c.Context(), scope, "read") {
				continue
			}
			scheme := "s3"
			if cred, err := bucketService.GetS3Credential(c.Context(), scope.CredentialID); err == nil && cred != nil {
				scheme = address.ProviderToScheme(cred.Provider)
			}
			path := fmt.Sprintf("%s://%s", scheme, scope.Bucket)
			if strings.TrimSpace(scope.PathPrefix) != "" {
				path = fmt.Sprintf("%s/%s", path, strings.Trim(strings.TrimSpace(scope.PathPrefix), "/"))
			}
			result = append(result, bucketapi.BucketScopeResponse{
				Organization: scope.Organization,
				ProjectId:    scope.ProjectID,
				Path:         &path,
			})
		}
	}
	return c.JSON(result)
}
