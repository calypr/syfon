package internaldrs

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

func handleInternalBucketsFiber(c fiber.Ctx, om *core.ObjectManager) error {
	creds, err := om.ListS3Credentials(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	scopes, _ := om.ListBucketScopes(c.Context())

	allowedBuckets := map[string]bool{}
	allowAll := bucketControlOpenAccess(c.Context(), "read")
	if !allowAll {
		if err := requireGen3AuthFiber(c); err != nil {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
		allowedBuckets = allowedBucketsForScopes(c.Context(), scopes, "read", "create", "update", "delete", "file_upload")
		if len(allowedBuckets) == 0 {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
	}

	resp := bucketapi.BucketsResponse{
		S3BUCKETS: map[string]bucketapi.BucketMetadata{},
	}
	outBuckets := resp.S3BUCKETS
	programsByBucket := map[string][]string{}
	for _, s := range scopes {
		if !allowAll && !allowedBuckets[s.Bucket] {
			continue
		}
		res := common.ResourcePathForScope(s.Organization, s.ProjectID)
		if res == "" {
			continue
		}
		programsByBucket[s.Bucket] = append(programsByBucket[s.Bucket], res)
	}
	for _, c := range creds {
		if !allowAll && !allowedBuckets[c.Bucket] {
			continue
		}
		meta := bucketapi.BucketMetadata{
			EndpointUrl: common.Ptr(c.Endpoint),
			Provider:    common.Ptr(c.Provider),
			Region:      common.Ptr(c.Region),
		}
		if programs := programsByBucket[c.Bucket]; len(programs) > 0 {
			meta.Programs = &programs
		}
		outBuckets[c.Bucket] = meta
	}

	return c.JSON(resp)
}

func handleInternalPutBucketFiber(c fiber.Ctx, om *core.ObjectManager) error {
	var req bucketapi.PutBucketRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}

	provider := strings.ToLower(strings.TrimSpace(common.StringVal(req.Provider)))
	switch provider {
	case "", "s3":
		provider = "s3"
	case "gs", "gcs":
		provider = "gcs"
	case "azure", "file":
		// keep as-is
	default:
		return c.Status(fiber.StatusBadRequest).SendString("provider must be one of: s3, gcs, azure, file")
	}

	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Bucket == "" || req.Organization == "" || req.ProjectId == "" {
		return c.Status(fiber.StatusBadRequest).SendString("bucket, organization, and project_id are required")
	}

	if !bucketControlAllowed(c.Context(), "create", "update") {
		if err := requireGen3AuthFiber(c); err != nil {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
		res := common.ResourcePathForScope(req.Organization, req.ProjectId)
		if res == "" || !resourceAllowed(c.Context(), res, "create", "update") {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
	}

	prefix, err := common.NormalizeStoragePath(readOptionalPath(req.Path), req.Bucket)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	if prefix == "" {
		prefix = strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}

	existingCred, credErr := om.GetS3Credential(c.Context(), req.Bucket)
	hasExistingCred := credErr == nil && existingCred != nil
	scopeOnly := hasExistingCred &&
		strings.TrimSpace(common.StringVal(req.AccessKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.SecretKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.Endpoint)) == "" &&
		strings.TrimSpace(common.StringVal(req.Region)) == "" &&
		strings.TrimSpace(common.StringVal(req.Provider)) == ""

	if !hasExistingCred && provider == "s3" &&
		(strings.TrimSpace(common.StringVal(req.AccessKey)) == "" || strings.TrimSpace(common.StringVal(req.SecretKey)) == "") {
		return c.Status(fiber.StatusBadRequest).SendString("access_key and secret_key are required for new s3 credentials")
	}

	if err := om.CreateBucketScope(c.Context(), &models.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       req.Bucket,
		PathPrefix:   prefix,
	}); err != nil {
		return apiutil.HandleError(c, err)
	}

	if scopeOnly {
		return c.SendStatus(fiber.StatusCreated)
	}

	region := strings.TrimSpace(common.StringVal(req.Region))
	accessKey := strings.TrimSpace(common.StringVal(req.AccessKey))
	secretKey := strings.TrimSpace(common.StringVal(req.SecretKey))
	endpoint := strings.TrimSpace(common.StringVal(req.Endpoint))
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

	cred := &models.S3Credential{
		Bucket:    req.Bucket,
		Provider:  provider,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  endpoint,
	}
	if provider == "s3" && (strings.TrimSpace(cred.AccessKey) == "" || strings.TrimSpace(cred.SecretKey) == "") {
		return c.Status(fiber.StatusBadRequest).SendString("access_key and secret_key are required for s3 credentials")
	}

	if err := om.SaveS3Credential(c.Context(), cred); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.SendStatus(fiber.StatusCreated)
}

func handleInternalDeleteBucketFiber(c fiber.Ctx, om *core.ObjectManager) error {
	bucket := c.Params("bucket")
	if bucket == "" {
		return c.Status(fiber.StatusBadRequest).SendString("bucket name is required")
	}

	if !bucketControlAllowed(c.Context(), "delete") {
		if err := requireGen3AuthFiber(c); err != nil {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
		scopes, err := om.ListBucketScopes(c.Context())
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		if !bucketsAllowedByNames(c.Context(), scopes, bucket, "delete", "update") {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
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
	if req.Organization == "" || req.ProjectId == "" {
		return c.Status(fiber.StatusBadRequest).SendString("organization and project_id are required")
	}

	if !bucketControlAllowed(c.Context(), "create", "update") {
		if err := requireGen3AuthFiber(c); err != nil {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
		res := common.ResourcePathForScope(req.Organization, req.ProjectId)
		if res == "" || !resourceAllowed(c.Context(), res, "create", "update") {
			return apiutil.HandleError(c, common.ErrUnauthorized)
		}
	}

	path := readOptionalPath(req.Path)
	if strings.TrimSpace(path) == "" {
		path = config.S3Prefix + bucket + "/" + strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}
	prefix, err := common.NormalizeStoragePath(path, bucket)
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

func readOptionalPath(path *string) string {
	if path == nil {
		return ""
	}
	return strings.TrimSpace(*path)
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return io.ErrUnexpectedEOF
	}
	return nil
}
