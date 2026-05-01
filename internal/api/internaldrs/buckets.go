package internaldrs

import (
	"strings"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

func registerInternalBucketRoutes(router fiber.Router, om *core.ObjectManager) {
	router.Get(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalBucketsFiber(c, om) })
	router.Put(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalPutBucketFiber(c, om) })
	router.Delete(routeutil.FiberPath(common.RouteInternalBucketDetail), func(c fiber.Ctx) error { return handleInternalDeleteBucketFiber(c, om) })
	router.Post(routeutil.FiberPath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalCreateBucketScopeFiber(c, om) })
}

func handleInternalBucketsFiber(c fiber.Ctx, om *core.ObjectManager) error {
	visible, err := om.ListVisibleBuckets(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	if apimiddleware.MissingGen3AuthHeader(c.Context()) {
		return apiutil.HandleError(c, common.ErrUnauthorized)
	}

	resp := bucketapi.BucketsResponse{S3BUCKETS: map[string]bucketapi.BucketMetadata{}}
	for _, entry := range visible {
		cred := entry.Credential
		meta := bucketapi.BucketMetadata{
			EndpointUrl: common.Ptr(cred.Endpoint),
			Provider:    common.Ptr(cred.Provider),
			Region:      common.Ptr(cred.Region),
		}
		if strings.TrimSpace(cred.BillingLogBucket) != "" {
			meta.BillingLogBucket = common.Ptr(cred.BillingLogBucket)
		}
		if strings.TrimSpace(cred.BillingLogPrefix) != "" {
			meta.BillingLogPrefix = common.Ptr(cred.BillingLogPrefix)
		}
		if len(entry.Programs) > 0 {
			programs := append([]string(nil), entry.Programs...)
			meta.Programs = &programs
		}
		resp.S3BUCKETS[cred.Bucket] = meta
	}
	return c.JSON(resp)
}

func handleInternalPutBucketFiber(c fiber.Ctx, om *core.ObjectManager) error {
	var req bucketapi.PutBucketRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}

	rawProvider := strings.TrimSpace(common.StringVal(req.Provider))
	bucketProvider, err := common.ParseBucketProvider(rawProvider)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("provider must be one of: s3, gcs, azure")
	}
	req.Provider = common.Ptr(bucketProvider)

	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Bucket == "" {
		return c.Status(fiber.StatusBadRequest).SendString("bucket is required")
	}
	if req.Organization == "" && req.ProjectId != "" {
		return c.Status(fiber.StatusBadRequest).SendString("organization is required when project_id is set")
	}
	if err := common.ValidateBucketName(bucketProvider, req.Bucket); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	if err := authorizeBucketScopeWrite(c.Context(), req.Organization, req.ProjectId, "create", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}

	prefix, err := common.NormalizeStoragePath(readOptionalPath(req.Path), req.Bucket)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}

	existingCred, credErr := om.GetS3Credential(c.Context(), req.Bucket)
	hasExistingCred := credErr == nil && existingCred != nil
	scopeOnly := hasExistingCred &&
		strings.TrimSpace(common.StringVal(req.AccessKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.SecretKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.Endpoint)) == "" &&
		strings.TrimSpace(common.StringVal(req.Region)) == "" &&
		strings.TrimSpace(common.StringVal(req.BillingLogBucket)) == "" &&
		strings.TrimSpace(common.StringVal(req.BillingLogPrefix)) == "" &&
		rawProvider == "" &&
		req.Organization != ""

	if !hasExistingCred && bucketProvider == common.S3Provider &&
		(strings.TrimSpace(common.StringVal(req.AccessKey)) == "" || strings.TrimSpace(common.StringVal(req.SecretKey)) == "") {
		return c.Status(fiber.StatusBadRequest).SendString("access_key and secret_key are required for new s3 credentials")
	}

	if req.Organization != "" {
		if err := om.CreateBucketScope(c.Context(), &models.BucketScope{
			Organization: req.Organization,
			ProjectID:    req.ProjectId,
			Bucket:       req.Bucket,
			PathPrefix:   prefix,
		}); err != nil {
			return apiutil.HandleError(c, err)
		}
	}
	if scopeOnly {
		return c.SendStatus(fiber.StatusCreated)
	}

	region := strings.TrimSpace(common.StringVal(req.Region))
	accessKey := strings.TrimSpace(common.StringVal(req.AccessKey))
	secretKey := strings.TrimSpace(common.StringVal(req.SecretKey))
	endpoint := strings.TrimSpace(common.StringVal(req.Endpoint))
	billingLogBucket := strings.TrimSpace(common.StringVal(req.BillingLogBucket))
	billingLogPrefix := strings.Trim(strings.TrimSpace(common.StringVal(req.BillingLogPrefix)), "/")
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
		if billingLogBucket == "" {
			billingLogBucket = existingCred.BillingLogBucket
		}
		if billingLogPrefix == "" {
			billingLogPrefix = existingCred.BillingLogPrefix
		}
	}

	cred := &models.S3Credential{
		Bucket:           req.Bucket,
		Provider:         bucketProvider,
		Region:           region,
		AccessKey:        accessKey,
		SecretKey:        secretKey,
		Endpoint:         endpoint,
		BillingLogBucket: billingLogBucket,
		BillingLogPrefix: billingLogPrefix,
	}
	if bucketProvider == common.S3Provider && (strings.TrimSpace(cred.AccessKey) == "" || strings.TrimSpace(cred.SecretKey) == "") {
		return c.Status(fiber.StatusBadRequest).SendString("access_key and secret_key are required for s3 credentials")
	}
	if err := om.SaveS3Credential(c.Context(), cred); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusCreated)
}

func handleInternalDeleteBucketFiber(c fiber.Ctx, om *core.ObjectManager) error {
	bucket := strings.TrimSpace(c.Params("bucket"))
	if bucket == "" {
		return c.Status(fiber.StatusBadRequest).SendString("bucket name is required")
	}
	if err := authorizeBucketDelete(c.Context(), om, bucket); err != nil {
		return apiutil.HandleError(c, err)
	}
	if err := om.DeleteS3Credential(c.Context(), bucket); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func handleInternalCreateBucketScopeFiber(c fiber.Ctx, om *core.ObjectManager) error {
	bucket := strings.TrimSpace(c.Params("bucket"))
	if bucket == "" {
		return c.Status(fiber.StatusBadRequest).SendString("bucket name is required")
	}
	if _, err := om.GetS3Credential(c.Context(), bucket); err != nil {
		return apiutil.HandleError(c, err)
	}

	var req bucketapi.AddBucketScopeRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Organization == "" {
		return c.Status(fiber.StatusBadRequest).SendString("organization is required")
	}
	if err := authorizeBucketScopeWrite(c.Context(), req.Organization, req.ProjectId, "create", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}

	prefix, err := common.NormalizeStoragePath(readOptionalPath(req.Path), bucket)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	if err := om.CreateBucketScope(c.Context(), &models.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       bucket,
		PathPrefix:   prefix,
	}); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusCreated)
}
