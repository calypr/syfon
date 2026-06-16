package internaldrs

import (
	"strings"

	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/core"
	"github.com/gofiber/fiber/v3"
)

type internalInspectObjectRequest struct {
	Organization string `json:"organization,omitempty"`
	Project      string `json:"project,omitempty"`
	Key          string `json:"key,omitempty"`
	Scheme       string `json:"scheme,omitempty"`
	ObjectURL    string `json:"object_url,omitempty"`
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
			Organization: strings.TrimSpace(req.Organization),
			Project:      strings.TrimSpace(req.Project),
			Key:          strings.TrimSpace(req.Key),
			Scheme:       strings.TrimSpace(req.Scheme),
			ObjectURL:    strings.TrimSpace(req.ObjectURL),
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

func handleInspectStorageError(c fiber.Ctx, err error) error {
	if inspectErr, ok := err.(*core.StorageInspectError); ok {
		switch inspectErr.Kind {
		case core.StorageInspectInvalidInput, core.StorageInspectUnsupported:
			return apiutil.Reject(c, fiber.StatusBadRequest, inspectErr.Error())
		case core.StorageInspectScopeNotFound, core.StorageInspectCredentialMissing, core.StorageInspectObjectNotFound:
			return apiutil.Reject(c, fiber.StatusNotFound, inspectErr.Error())
		case core.StorageInspectPermissionDenied:
			return apiutil.Reject(c, fiber.StatusForbidden, inspectErr.Error())
		}
	}
	return apiutil.HandleError(c, err)
}
