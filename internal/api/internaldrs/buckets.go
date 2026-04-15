package internaldrs

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/bucketapi"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/api/routeutil"
)

func handleInternalBuckets(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	scopes, _ := database.ListBucketScopes(r.Context())

	allowedBuckets := map[string]bool{}
	allowAll := !core.IsGen3Mode(r.Context()) || hasGlobalBucketControlAccess(r, "read")
	if !allowAll {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		for _, s := range scopes {
			if hasScopedBucketAccess(r, s, "read", "create", "update", "delete", "file_upload") {
				allowedBuckets[s.Bucket] = true
			}
		}
		if len(allowedBuckets) == 0 {
			writeAuthError(w, r)
			return
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
		res := core.ResourcePathForScope(s.Organization, s.ProjectID)
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
			EndpointUrl: core.Ptr(c.Endpoint),
			Provider:    core.Ptr(c.Provider),
			Region:      core.Ptr(c.Region),
		}
		if programs := programsByBucket[c.Bucket]; len(programs) > 0 {
			meta.Programs = &programs
		}
		outBuckets[c.Bucket] = meta
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, "Failed to encode response", err)
	}
}

func handleInternalPutBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req bucketapi.PutBucketRequest
	if err := decodeStrictJSON(r.Body, &req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}

	provider := strings.ToLower(strings.TrimSpace(core.StringVal(req.Provider)))
	switch provider {
	case "", "s3":
		provider = "s3"
	case "gs", "gcs":
		provider = "gcs"
	case "azure", "file":
		// keep as-is
	default:
		writeHTTPError(w, r, http.StatusBadRequest, "provider must be one of: s3, gcs, azure, file", nil)
		return
	}

	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if v := strings.TrimSpace(core.StringVal(req.Region)); v != "" {
		req.Region = core.Ptr(v)
	}
	if v := strings.TrimSpace(core.StringVal(req.AccessKey)); v != "" {
		req.AccessKey = core.Ptr(v)
	}
	if v := strings.TrimSpace(core.StringVal(req.SecretKey)); v != "" {
		req.SecretKey = core.Ptr(v)
	}
	if v := strings.TrimSpace(core.StringVal(req.Endpoint)); v != "" {
		req.Endpoint = core.Ptr(v)
	}
	if req.Bucket == "" || req.Organization == "" || req.ProjectId == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket, organization, and project_id are required", nil)
		return
	}

	if core.IsGen3Mode(r.Context()) {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if !hasGlobalBucketControlAccess(r, "create", "update") {
			res := scopeResource(req.Organization, req.ProjectId)
			if res == "" || !hasAnyMethodAccess(r, []string{res}, "create", "update") {
				writeAuthError(w, r)
				return
			}
		}
	}

	prefix, err := normalizeScopePath(readOptionalPath(req.Path), req.Bucket)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	if prefix == "" {
		prefix = strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}

	existingCred, credErr := database.GetS3Credential(r.Context(), req.Bucket)
	hasExistingCred := credErr == nil && existingCred != nil
	scopeOnly := hasExistingCred &&
		strings.TrimSpace(core.StringVal(req.AccessKey)) == "" &&
		strings.TrimSpace(core.StringVal(req.SecretKey)) == "" &&
		strings.TrimSpace(core.StringVal(req.Endpoint)) == "" &&
		strings.TrimSpace(core.StringVal(req.Region)) == "" &&
		strings.TrimSpace(core.StringVal(req.Provider)) == ""

	if !hasExistingCred && provider == "s3" &&
		(strings.TrimSpace(core.StringVal(req.AccessKey)) == "" || strings.TrimSpace(core.StringVal(req.SecretKey)) == "") {
		writeHTTPError(w, r, http.StatusBadRequest, "access_key and secret_key are required for new s3 credentials", nil)
		return
	}

	if err := database.CreateBucketScope(r.Context(), &core.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       req.Bucket,
		PathPrefix:   prefix,
	}); err != nil {
		writeDBError(w, r, err)
		return
	}

	if scopeOnly {
		w.WriteHeader(http.StatusCreated)
		return
	}

	region := strings.TrimSpace(core.StringVal(req.Region))
	accessKey := strings.TrimSpace(core.StringVal(req.AccessKey))
	secretKey := strings.TrimSpace(core.StringVal(req.SecretKey))
	endpoint := strings.TrimSpace(core.StringVal(req.Endpoint))
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

	cred := &core.S3Credential{
		Bucket:    req.Bucket,
		Provider:  provider,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  endpoint,
	}
	if provider == "s3" && (strings.TrimSpace(cred.AccessKey) == "" || strings.TrimSpace(cred.SecretKey) == "") {
		writeHTTPError(w, r, http.StatusBadRequest, "access_key and secret_key are required for s3 credentials", nil)
		return
	}

	if err := database.SaveS3Credential(r.Context(), cred); err != nil {
		writeDBError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func handleInternalDeleteBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	bucket := routeutil.PathParam(r, "bucket")
	if bucket == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket name is required", nil)
		return
	}

	if core.IsGen3Mode(r.Context()) {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if !hasGlobalBucketControlAccess(r, "delete") {
			scopes, err := database.ListBucketScopes(r.Context())
			if err != nil {
				writeDBError(w, r, err)
				return
			}
			matching := 0
			for _, s := range scopes {
				if s.Bucket != bucket {
					continue
				}
				matching++
				if !hasScopedBucketAccess(r, s, "delete", "update") {
					writeAuthError(w, r)
					return
				}
			}
			if matching == 0 {
				writeAuthError(w, r)
				return
			}
		}
	}

	if err := database.DeleteS3Credential(r.Context(), bucket); err != nil {
		writeDBError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleInternalCreateBucketScope(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	bucket := strings.TrimSpace(routeutil.PathParam(r, "bucket"))
	if bucket == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket name is required", nil)
		return
	}
	if _, err := database.GetS3Credential(r.Context(), bucket); err != nil {
		writeDBError(w, r, err)
		return
	}

	var req bucketapi.AddBucketScopeRequest
	if err := decodeStrictJSON(r.Body, &req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	req.Organization = strings.TrimSpace(req.Organization)
	req.ProjectId = strings.TrimSpace(req.ProjectId)
	if req.Organization == "" || req.ProjectId == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "organization and project_id are required", nil)
		return
	}

	if core.IsGen3Mode(r.Context()) {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if !hasGlobalBucketControlAccess(r, "create", "update") {
			res := scopeResource(req.Organization, req.ProjectId)
			if res == "" || !hasAnyMethodAccess(r, []string{res}, "create", "update") {
				writeAuthError(w, r)
				return
			}
		}
	}

	path := readOptionalPath(req.Path)
	if strings.TrimSpace(path) == "" {
		path = config.S3Prefix + bucket + "/" + strings.Trim(req.Organization+"/"+req.ProjectId, "/")
	}
	prefix, err := normalizeScopePath(path, bucket)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	if err := database.CreateBucketScope(r.Context(), &core.BucketScope{
		Organization: req.Organization,
		ProjectID:    req.ProjectId,
		Bucket:       bucket,
		PathPrefix:   prefix,
	}); err != nil {
		writeDBError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func readOptionalPath(path *string) string {
	if path == nil {
		return ""
	}
	return strings.TrimSpace(*path)
}

func decodeStrictJSON(body io.Reader, dst any) error {
	dec := json.NewDecoder(body)
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
