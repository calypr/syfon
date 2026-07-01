package internaldrs

import (
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

type internalInspectObjectRequest struct {
	ID                string `json:"id,omitempty"`
	Organization      string `json:"organization,omitempty"`
	Project           string `json:"project,omitempty"`
	Key               string `json:"key,omitempty"`
	Scheme            string `json:"scheme,omitempty"`
	ObjectURL         string `json:"object_url,omitempty"`
	ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
	ExpectedSHA256    string `json:"expected_sha256,omitempty"`
}

type internalInspectObjectsBulkRequest struct {
	Items []internalInspectObjectRequest `json:"items"`
}

type internalInspectProjectBucketRequest struct {
	Organization string `json:"organization,omitempty"`
	Project      string `json:"project,omitempty"`
	IncludeHead  bool   `json:"include_head,omitempty"`
}

type internalInspectProjectRecordsRequest struct {
	Organization string `json:"organization,omitempty"`
	Project      string `json:"project,omitempty"`
}

type internalInspectProjectScopesRequest struct {
	Organization string `json:"organization,omitempty"`
	Project      string `json:"project,omitempty"`
}

type internalDeleteProjectBucketObjectsRequest struct {
	Organization string   `json:"organization,omitempty"`
	Project      string   `json:"project,omitempty"`
	ObjectURLs   []string `json:"object_urls"`
}

type internalInspectObjectResponse struct {
	ObjectURL   string `json:"object_url"`
	Provider    string `json:"provider"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	MetaSHA256  string `json:"meta_sha256,omitempty"`
	ETag        string `json:"etag,omitempty"`
	LastModTime string `json:"last_modified,omitempty"`
}

type internalInspectObjectBulkResponse struct {
	Items []internalInspectObjectBulkItem `json:"items"`
}

type internalInspectProjectBucketResponse struct {
	Items []internalInspectProjectBucketItem `json:"items"`
}

type internalInspectProjectRecordsResponse struct {
	Items []internalInspectProjectRecordItem `json:"items"`
}

type internalInspectProjectScopesResponse struct {
	Items []internalInspectProjectScopeItem `json:"items"`
}

type internalDeleteProjectBucketObjectsResponse struct {
	Items []internalDeleteProjectBucketObjectsItem `json:"items"`
}

type internalInspectObjectBulkItem struct {
	ID                   string   `json:"id,omitempty"`
	ObjectURL            string   `json:"object_url,omitempty"`
	Provider             string   `json:"provider,omitempty"`
	Bucket               string   `json:"bucket,omitempty"`
	Key                  string   `json:"key,omitempty"`
	Path                 string   `json:"path,omitempty"`
	Exists               bool     `json:"exists"`
	Status               string   `json:"status"`
	Error                string   `json:"error,omitempty"`
	ErrorKind            string   `json:"error_kind,omitempty"`
	SizeBytes            *int64   `json:"size_bytes,omitempty"`
	MetaSHA256           string   `json:"meta_sha256,omitempty"`
	ETag                 string   `json:"etag,omitempty"`
	LastModTime          string   `json:"last_modified,omitempty"`
	ValidationStatus     string   `json:"validation_status"`
	SizeMatch            *bool    `json:"size_match,omitempty"`
	SHA256Match          *bool    `json:"sha256_match,omitempty"`
	ValidationMismatches []string `json:"validation_mismatches,omitempty"`
}

type internalInspectProjectBucketItem struct {
	ObjectURL   string `json:"object_url"`
	Provider    string `json:"provider"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	MetaSHA256  string `json:"meta_sha256,omitempty"`
	ETag        string `json:"etag,omitempty"`
	LastModTime string `json:"last_modified,omitempty"`
}

type internalInspectProjectRecordItem struct {
	ObjectID      string                        `json:"object_id"`
	Checksum      string                        `json:"checksum"`
	Organization  string                        `json:"organization"`
	Project       string                        `json:"project"`
	Size          int64                         `json:"size"`
	CreatedTime   string                        `json:"created_time,omitempty"`
	UpdatedTime   string                        `json:"updated_time,omitempty"`
	AccessURLs    []string                      `json:"access_urls"`
	AccessMethods []internalProjectAccessMethod `json:"access_methods"`
}

type internalProjectAccessMethod struct {
	AccessID string   `json:"access_id,omitempty"`
	Type     string   `json:"type,omitempty"`
	URL      string   `json:"url,omitempty"`
	Headers  []string `json:"headers,omitempty"`
}

type internalInspectProjectScopeItem struct {
	Bucket       string `json:"bucket"`
	Organization string `json:"organization"`
	ProjectID    string `json:"project_id,omitempty"`
	Path         string `json:"path,omitempty"`
}

type internalDeleteProjectBucketObjectsItem struct {
	ObjectURL string `json:"object_url"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

func handleInternalInspectObjectFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalInspectObjectRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		resp, err := om.InspectStorageObject(c.Context(), core.InspectStorageRequest{
			ID:                strings.TrimSpace(req.ID),
			Organization:      strings.TrimSpace(req.Organization),
			Project:           strings.TrimSpace(req.Project),
			Key:               strings.TrimSpace(req.Key),
			Scheme:            strings.TrimSpace(req.Scheme),
			ObjectURL:         strings.TrimSpace(req.ObjectURL),
			ExpectedSizeBytes: req.ExpectedSizeBytes,
			ExpectedSHA256:    strings.TrimSpace(req.ExpectedSHA256),
		})
		if err != nil {
			return handleInspectStorageError(c, err)
		}
		out := internalInspectObjectResponse{
			ObjectURL:  resp.ObjectURL,
			Provider:   resp.Provider,
			Bucket:     resp.Bucket,
			Key:        resp.Key,
			Path:       resp.Path,
			SizeBytes:  resp.SizeBytes,
			MetaSHA256: resp.MetaSHA256,
			ETag:       resp.ETag,
		}
		if !resp.LastModTime.IsZero() {
			out.LastModTime = resp.LastModTime.Format("2006-01-02T15:04:05Z07:00")
		}
		return c.JSON(out)
	}
}

func handleInternalInspectObjectBulkFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalInspectObjectsBulkRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		if len(req.Items) == 0 {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: items are required")
		}
		items := make([]core.InspectStorageRequest, 0, len(req.Items))
		for _, item := range req.Items {
			items = append(items, core.InspectStorageRequest{
				ID:                strings.TrimSpace(item.ID),
				Organization:      strings.TrimSpace(item.Organization),
				Project:           strings.TrimSpace(item.Project),
				Key:               strings.TrimSpace(item.Key),
				Scheme:            strings.TrimSpace(item.Scheme),
				ObjectURL:         strings.TrimSpace(item.ObjectURL),
				ExpectedSizeBytes: item.ExpectedSizeBytes,
				ExpectedSHA256:    strings.TrimSpace(item.ExpectedSHA256),
			})
		}
		results := om.InspectStorageObjects(c.Context(), items)
		out := internalInspectObjectBulkResponse{Items: make([]internalInspectObjectBulkItem, 0, len(results))}
		for _, result := range results {
			out.Items = append(out.Items, bulkInspectItemFromCore(result))
		}
		return c.JSON(out)
	}
}

func handleInternalInspectProjectBucketFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalInspectProjectBucketRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		items, err := om.ListProjectStorageObjects(c.Context(), strings.TrimSpace(req.Organization), strings.TrimSpace(req.Project), req.IncludeHead)
		if err != nil {
			return handleInspectStorageError(c, err)
		}
		out := internalInspectProjectBucketResponse{Items: make([]internalInspectProjectBucketItem, 0, len(items))}
		for _, item := range items {
			row := internalInspectProjectBucketItem{
				ObjectURL:  item.ObjectURL,
				Provider:   item.Provider,
				Bucket:     item.Bucket,
				Key:        item.Key,
				Path:       item.Path,
				SizeBytes:  item.SizeBytes,
				MetaSHA256: item.MetaSHA256,
				ETag:       item.ETag,
			}
			if !item.LastModTime.IsZero() {
				row.LastModTime = item.LastModTime.Format(time.RFC3339)
			}
			out.Items = append(out.Items, row)
		}
		return c.JSON(out)
	}
}

func handleInternalInspectProjectRecordsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalInspectProjectRecordsRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		organization := strings.TrimSpace(req.Organization)
		project := strings.TrimSpace(req.Project)
		if organization == "" || project == "" {
			return apiutil.Reject(c, fiber.StatusBadRequest, "organization and project are required")
		}
		objects, err := om.ListObjectsByScope(c.Context(), organization, project, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		out := internalInspectProjectRecordsResponse{Items: make([]internalInspectProjectRecordItem, 0, len(objects))}
		for _, obj := range objects {
			record, ok := projectRecordAuditItemFromObject(obj, organization, project)
			if !ok {
				continue
			}
			out.Items = append(out.Items, record)
		}
		return c.JSON(out)
	}
}

func handleInternalInspectProjectScopesFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalInspectProjectScopesRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		organization := strings.TrimSpace(req.Organization)
		project := strings.TrimSpace(req.Project)
		if organization == "" || project == "" {
			return apiutil.Reject(c, fiber.StatusBadRequest, "organization and project are required")
		}
		scopes, err := om.ListBucketScopes(c.Context())
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		out := internalInspectProjectScopesResponse{Items: make([]internalInspectProjectScopeItem, 0)}
		for _, scope := range scopes {
			if !strings.EqualFold(strings.TrimSpace(scope.Organization), organization) {
				continue
			}
			scopeProject := strings.TrimSpace(scope.ProjectID)
			if scopeProject != "" && !strings.EqualFold(scopeProject, project) {
				continue
			}
			if !bucketScopeAllowed(c.Context(), scope, "read") {
				continue
			}
			row := internalInspectProjectScopeItem{
				Bucket:       strings.TrimSpace(scope.Bucket),
				Organization: organization,
				ProjectID:    scopeProject,
			}
			scheme := "s3"
			if cred, err := om.GetS3Credential(c.Context(), scope.CredentialID); err == nil && cred != nil {
				scheme = common.ProviderToScheme(cred.Provider)
			}
			row.Path = scheme + "://" + strings.TrimSpace(scope.Bucket)
			if scope.PathPrefix != "" {
				row.Path += "/" + strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
			}
			out.Items = append(out.Items, row)
		}
		return c.JSON(out)
	}
}

func handleInternalDeleteProjectBucketObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req internalDeleteProjectBucketObjectsRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		if len(req.ObjectURLs) == 0 {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: object_urls are required")
		}
		results := om.DeleteProjectStorageObjects(c.Context(), strings.TrimSpace(req.Organization), strings.TrimSpace(req.Project), req.ObjectURLs)
		out := internalDeleteProjectBucketObjectsResponse{
			Items: make([]internalDeleteProjectBucketObjectsItem, 0, len(results)),
		}
		for _, result := range results {
			out.Items = append(out.Items, internalDeleteProjectBucketObjectsItem{
				ObjectURL: result.ObjectURL,
				Status:    result.Status,
				Error:     result.Error,
			})
		}
		return c.JSON(out)
	}
}

func handleInspectStorageError(c fiber.Ctx, err error) error {
	if inspectErr, ok := err.(*core.StorageInspectError); ok {
		switch inspectErr.Kind {
		case core.StorageInspectInvalidInput, core.StorageInspectUnsupported:
			return apiutil.Reject(c, fiber.StatusBadRequest, inspectErr.Error())
		case core.StorageInspectScopeNotFound, core.StorageInspectCredentialMissing, core.StorageInspectObjectNotFound:
			return apiutil.Reject(c, fiber.StatusNotFound, inspectErr.Error())
		case core.StorageInspectBucketUnavailable:
			return apiutil.Reject(c, fiber.StatusConflict, inspectErr.Error())
		case core.StorageInspectPermissionDenied:
			return apiutil.Reject(c, fiber.StatusForbidden, inspectErr.Error())
		}
	}
	return apiutil.HandleError(c, err)
}

func bulkInspectItemFromCore(result core.StorageProbeResult) internalInspectObjectBulkItem {
	out := internalInspectObjectBulkItem{
		ID:                   result.ID,
		ObjectURL:            result.ObjectURL,
		Provider:             result.Provider,
		Bucket:               result.Bucket,
		Key:                  result.Key,
		Path:                 result.Path,
		Exists:               result.Exists,
		Status:               string(result.Status),
		Error:                result.Error,
		ErrorKind:            result.ErrorKind,
		SizeBytes:            result.SizeBytes,
		MetaSHA256:           result.MetaSHA256,
		ETag:                 result.ETag,
		ValidationStatus:     string(result.ValidationStatus),
		SizeMatch:            result.SizeMatch,
		SHA256Match:          result.SHA256Match,
		ValidationMismatches: append([]string(nil), result.ValidationMismatches...),
	}
	if !result.LastModTime.IsZero() {
		out.LastModTime = result.LastModTime.Format(time.RFC3339)
	}
	return out
}

func projectRecordAuditItemFromObject(obj models.InternalObject, organization, project string) (internalInspectProjectRecordItem, bool) {
	checksum := primarySHA256Checksum(obj.Checksums)
	if checksum == "" {
		return internalInspectProjectRecordItem{}, false
	}
	accessURLs := make([]string, 0)
	accessMethods := make([]internalProjectAccessMethod, 0)
	if obj.AccessMethods != nil {
		for _, method := range *obj.AccessMethods {
			row := internalProjectAccessMethod{
				Type:    strings.TrimSpace(string(method.Type)),
				Headers: []string{},
			}
			if method.AccessId != nil {
				row.AccessID = strings.TrimSpace(*method.AccessId)
			}
			if method.AccessUrl != nil {
				row.URL = strings.TrimSpace(method.AccessUrl.Url)
				if row.URL != "" {
					accessURLs = append(accessURLs, row.URL)
				}
				if method.AccessUrl.Headers != nil {
					row.Headers = append([]string(nil), (*method.AccessUrl.Headers)...)
				}
			}
			accessMethods = append(accessMethods, row)
		}
	}
	item := internalInspectProjectRecordItem{
		ObjectID:      strings.TrimSpace(obj.Id),
		Checksum:      checksum,
		Organization:  organization,
		Project:       project,
		Size:          obj.Size,
		AccessURLs:    accessURLs,
		AccessMethods: accessMethods,
	}
	if !obj.CreatedTime.IsZero() {
		item.CreatedTime = obj.CreatedTime.Format(time.RFC3339Nano)
	}
	if obj.UpdatedTime != nil && !obj.UpdatedTime.IsZero() {
		item.UpdatedTime = obj.UpdatedTime.Format(time.RFC3339Nano)
	}
	return item, true
}

func primarySHA256Checksum(checksums []drs.Checksum) string {
	for _, checksum := range checksums {
		if strings.EqualFold(strings.TrimSpace(checksum.Type), "sha256") {
			value := strings.TrimSpace(strings.TrimPrefix(checksum.Checksum, "sha256:"))
			if value != "" {
				return value
			}
		}
	}
	return ""
}
