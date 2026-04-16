package internaldrs

import (
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/internaldrs/logic"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
)

// --- Domain Mapping Tools ---

func canonicalIDFromInternal(req *internalapi.InternalRecord) string {
	if req == nil {
		return ""
	}
	return strings.TrimSpace(req.Did)
}

func internalToDrs(req *internalapi.InternalRecord) (*core.InternalObject, error) {
	now := time.Now()
	obj := &drs.DrsObject{
		Id:          canonicalIDFromInternal(req),
		SelfUri:     config.DRSPrefix + canonicalIDFromInternal(req),
		Size:        0,
		CreatedTime: now,
		UpdatedTime: &now,
	}
	if req == nil {
		return &core.InternalObject{DrsObject: *obj}, nil
	}
	if canonicalIDFromInternal(req) == "" {
		return nil, fmt.Errorf("did is required")
	}
	if len(req.Authz) == 0 {
		return nil, fmt.Errorf("authz is required")
	}
	if req.Size != nil {
		obj.Size = *req.Size
	}
	if req.FileName != nil && strings.TrimSpace(*req.FileName) != "" {
		obj.Name = req.FileName
	}
	if req.Version != nil && strings.TrimSpace(*req.Version) != "" {
		obj.Version = req.Version
	}
	if req.Description != nil && strings.TrimSpace(*req.Description) != "" {
		obj.Description = req.Description
	}
	if req.CreatedTime != nil {
		if ct := strings.TrimSpace(*req.CreatedTime); ct != "" {
			if t, err := time.Parse(time.RFC3339, ct); err == nil {
				obj.CreatedTime = t
			}
		}
	}
	if req.UpdatedTime != nil {
		if ut := strings.TrimSpace(*req.UpdatedTime); ut != "" {
			if t, err := time.Parse(time.RFC3339, ut); err == nil {
				obj.UpdatedTime = &t
			}
		}
	}
	if req.Hashes != nil {
		for t, v := range *req.Hashes {
			obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
		}
	}
	var methods []drs.AccessMethod
	if req.Urls != nil {
		methods = make([]drs.AccessMethod, 0, len(*req.Urls))
		for _, u := range *req.Urls {
			methods = append(methods, drs.AccessMethod{
				Type: drs.AccessMethodType("s3"),
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: u},
				Region: core.Ptr(config.DefaultS3Region),
			})
		}
		obj.AccessMethods = &methods
	}
	authz := append([]string(nil), req.Authz...)
	return &core.InternalObject{DrsObject: *obj, Authorizations: authz}, nil
}

func drsToInternalRecord(obj *core.InternalObject) *internalapi.InternalRecord {
	hashes := make(map[string]string, len(obj.Checksums))
	for _, c := range obj.Checksums {
		hashes[c.Type] = c.Checksum
	}

	var urls []string
	authz := append([]string(nil), obj.Authorizations...)
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl != nil && am.AccessUrl.Url != "" {
				urls = append(urls, am.AccessUrl.Url)
			}
		}
	}
	scope := core.ParseResourcePath(firstAuthz(authz))
	resp := &internalapi.InternalRecord{
		Authz: authz,
		Did:   obj.Id,
		Size:  &obj.Size,
	}
	if !obj.CreatedTime.IsZero() {
		v := obj.CreatedTime.Format(time.RFC3339)
		resp.CreatedTime = &v
	}
	if obj.UpdatedTime != nil {
		v := obj.UpdatedTime.Format(time.RFC3339)
		resp.UpdatedTime = &v
	}
	if name := core.StringVal(obj.Name); name != "" {
		resp.FileName = &name
	}
	if len(hashes) > 0 {
		h := internalapi.HashInfo(hashes)
		resp.Hashes = &h
	}
	if scope.Organization != "" {
		resp.Organization = &scope.Organization
	}
	if scope.Project != "" {
		resp.Project = &scope.Project
	}
	if len(urls) > 0 {
		resp.Urls = &urls
	}
	if version := core.StringVal(obj.Version); version != "" {
		resp.Version = &version
	}
	if desc := core.StringVal(obj.Description); desc != "" {
		resp.Description = &desc
	}
	return resp
}

func drsToInternal(obj *core.InternalObject) *internalapi.InternalRecordResponse {
	hashes := make(map[string]string, len(obj.Checksums))
	for _, c := range obj.Checksums {
		hashes[c.Type] = c.Checksum
	}

	var urls []string
	authz := append([]string(nil), obj.Authorizations...)
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl != nil && am.AccessUrl.Url != "" {
				urls = append(urls, am.AccessUrl.Url)
			}
		}
	}
	scope := core.ParseResourcePath(firstAuthz(authz))
	resp := &internalapi.InternalRecordResponse{
		Authz: authz,
		Did:   obj.Id,
		Size:  &obj.Size,
	}
	if name := core.StringVal(obj.Name); name != "" {
		resp.FileName = &name
	}
	if version := core.StringVal(obj.Version); version != "" {
		resp.Version = &version
	}
	if desc := core.StringVal(obj.Description); desc != "" {
		resp.Description = &desc
	}
	if len(hashes) > 0 {
		h := internalapi.HashInfo(hashes)
		resp.Hashes = &h
	}
	if len(urls) > 0 {
		resp.Urls = &urls
	}
	if scope.Organization != "" {
		resp.Organization = &scope.Organization
	}
	if scope.Project != "" {
		resp.Project = &scope.Project
	}
	createdTime := obj.CreatedTime.Format(time.RFC3339)
	resp.CreatedTime = &createdTime
	resp.CreatedDate = &createdTime

	if obj.UpdatedTime != nil {
		updatedTime := obj.UpdatedTime.Format(time.RFC3339)
		resp.UpdatedTime = &updatedTime
		resp.UpdatedDate = &updatedTime
	}
	return resp
}

// --- Path and URL Helpers ---

func normalizeScopePath(rawPath, bucket string) (string, error) {
	return logic.NormalizeScopePath(rawPath, bucket)
}

func objectURLForCredential(cred *core.S3Credential, key string) (string, error) {
	return logic.ObjectURLForCredential(cred, key)
}

// --- Internal Validation and Normalization Helpers ---

func providerForCredential(cred *core.S3Credential) string {
	return logic.ProviderForCredential(cred)
}

func normalizeHashQueryValue(raw string) string {
	clean := strings.Trim(strings.TrimSpace(raw), `"'`)
	if parts := strings.SplitN(clean, ":", 2); len(parts) == 2 {
		return strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}
	return clean
}

func normalizeHashQueryType(raw string) string {
	clean := strings.Trim(strings.TrimSpace(raw), `"'`)
	clean = strings.ToLower(clean)
	clean = strings.ReplaceAll(clean, "-", "")
	return clean
}

// parseHashQuery returns normalized hash type (when provided) and normalized value.
// Type precedence: explicit query `hash_type` first, then `type:value` prefix in `hash`.
func parseHashQuery(rawHash string, rawType string) (string, string) {
	return logic.ParseHashQuery(rawHash, rawType)
}

func objectHasChecksumTypeAndValue(obj core.InternalObject, hashType string, hashValue string) bool {
	return logic.ObjectHasChecksumTypeAndValue(obj, hashType, hashValue)
}

func looksLikeSHA256(v string) bool {
	return logic.LooksLikeSHA256(v)
}

func checksumHintFromInputs(guid, fileName string) string {
	return logic.ChecksumHintFromInputs(guid, fileName)
}

func targetResourcesFromObject(obj *core.InternalObject) []string {
	return logic.TargetResourcesFromObject(obj)
}

func firstAuthz(authz []string) string {
	return logic.FirstAuthz(authz)
}
