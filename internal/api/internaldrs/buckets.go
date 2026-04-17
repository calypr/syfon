package internaldrs

import (
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"

	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/internal/config"
	"github.com/gofiber/fiber/v3"
)

func handleInternalBucketsFiber(c fiber.Ctx, database db.CredentialStore) error {
	creds, err := database.ListS3Credentials(c.Context())
	if err != nil {
		return writeHTTPErrorFiber(c, http.StatusInternalServerError, err.Error(), err)
	}
	scopes, _ := database.ListBucketScopes(c.Context())

	allowedBuckets := map[string]bool{}
	allowAll := !authz.IsGen3Mode(c.Context()) || authz.HasGlobalBucketControlAccess(c.Context(), "read")
	if !allowAll {
		if !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}
		for _, s := range scopes {
			if authz.HasScopedBucketAccess(c.Context(), s, "read", "create", "update", "delete", "file_upload") {
				allowedBuckets[s.Bucket] = true
			}
		}
		if len(allowedBuckets) == 0 {
			return writeAuthErrorFiber(c)
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

func handleInternalPutBucketFiber(c fiber.Ctx, database db.CredentialStore) error {
	var req bucketapi.PutBucketRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "Invalid request body", nil)
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
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "provider must be one of: s3, gcs, azure, file", nil)
	}

	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if v := strings.TrimSpace(common.StringVal(req.Region)); v != "" {
		req.Region = common.Ptr(v)
	}
	if v := strings.TrimSpace(common.StringVal(req.AccessKey)); v != "" {
		req.AccessKey = common.Ptr(v)
	}
	if v := strings.TrimSpace(common.StringVal(req.SecretKey)); v != "" {
		req.SecretKey = common.Ptr(v)
	}
	if v := strings.TrimSpace(common.StringVal(req.Endpoint)); v != "" {
		req.Endpoint = common.Ptr(v)
	}
	if req.Bucket == "" || req.Organization == "" || req.ProjectId == "" {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "bucket, organization, and project_id are required", nil)
	}

	if authz.IsGen3Mode(c.Context()) {
		if !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}
		if !authz.HasGlobalBucketControlAccess(c.Context(), "create", "update") {
			res := common.ResourcePathForScope(req.Organization, req.ProjectId)
			if res == "" || !authz.HasAnyMethodAccess(c.Context(), []string{res}, "create", "update") {
				return writeAuthErrorFiber(c)
			}
		}
	}

	prefix, err := common.NormalizeStoragePath(readOptionalPath(req.Path), req.Bucket)
	if err != nil {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, err.Error(), err)
	}
	if prefix == "" {
		prefix = strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}

	existingCred, credErr := database.GetS3Credential(c.Context(), req.Bucket)
	hasExistingCred := credErr == nil && existingCred != nil
	scopeOnly := hasExistingCred &&
		strings.TrimSpace(common.StringVal(req.AccessKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.SecretKey)) == "" &&
		strings.TrimSpace(common.StringVal(req.Endpoint)) == "" &&
		strings.TrimSpace(common.StringVal(req.Region)) == "" &&
		strings.TrimSpace(common.StringVal(req.Provider)) == ""

	if !hasExistingCred && provider == "s3" &&
		(strings.TrimSpace(common.StringVal(req.AccessKey)) == "" || strings.TrimSpace(common.StringVal(req.SecretKey)) == "") {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "access_key and secret_key are required for new s3 credentials", nil)
	}

	if err := database.CreateBucketScope(c.Context(), &models.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       req.Bucket,
		PathPrefix:   prefix,
	}); err != nil {
		return writeDBErrorFiber(c, err)
	}

	if scopeOnly {
		return c.SendStatus(http.StatusCreated)
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
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "access_key and secret_key are required for s3 credentials", nil)
	}

	if err := database.SaveS3Credential(c.Context(), cred); err != nil {
		return writeDBErrorFiber(c, err)
	}
	return c.SendStatus(http.StatusCreated)
}

func handleInternalDeleteBucketFiber(c fiber.Ctx, database db.CredentialStore) error {
	bucket := c.Params("bucket")
	if bucket == "" {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "bucket name is required", nil)
	}

	if authz.IsGen3Mode(c.Context()) {
		if !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}
		if !authz.HasGlobalBucketControlAccess(c.Context(), "delete") {
			scopes, err := database.ListBucketScopes(c.Context())
			if err != nil {
				return writeDBErrorFiber(c, err)
			}
			matching := 0
			for _, s := range scopes {
				if s.Bucket != bucket {
					continue
				}
				matching++
				if !authz.HasScopedBucketAccess(c.Context(), s, "delete", "update") {
					return writeAuthErrorFiber(c)
				}
			}
			if matching == 0 {
				return writeAuthErrorFiber(c)
			}
		}
	}

	if err := database.DeleteS3Credential(c.Context(), bucket); err != nil {
		return writeDBErrorFiber(c, err)
	}
	return c.SendStatus(http.StatusNoContent)
}

func handleInternalCreateBucketScopeFiber(c fiber.Ctx, database db.CredentialStore) error {
	bucket := strings.TrimSpace(c.Params("bucket"))
	if bucket == "" {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "bucket name is required", nil)
	}
	if _, err := database.GetS3Credential(c.Context(), bucket); err != nil {
		return writeDBErrorFiber(c, err)
	}

	var req bucketapi.AddBucketScopeRequest
	if err := decodeStrictJSON(c.Body(), &req); err != nil {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "Invalid request body", nil)
	}
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Organization == "" || req.ProjectId == "" {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "organization and project_id are required", nil)
	}

	if authz.IsGen3Mode(c.Context()) {
		if !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}
		if !authz.HasGlobalBucketControlAccess(c.Context(), "create", "update") {
			res := common.ResourcePathForScope(req.Organization, req.ProjectId)
			if res == "" || !authz.HasAnyMethodAccess(c.Context(), []string{res}, "create", "update") {
				return writeAuthErrorFiber(c)
			}
		}
	}

	path := readOptionalPath(req.Path)
	if strings.TrimSpace(path) == "" {
		path = config.S3Prefix + bucket + "/" + strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}
	prefix, err := common.NormalizeStoragePath(path, bucket)
	if err != nil {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, err.Error(), err)
	}
	if err := database.CreateBucketScope(c.Context(), &models.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       bucket,
		PathPrefix:   prefix,
	}); err != nil {
		return writeDBErrorFiber(c, err)
	}
	return c.SendStatus(http.StatusCreated)
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
