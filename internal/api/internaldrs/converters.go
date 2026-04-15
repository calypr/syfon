package internaldrs

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/provider"
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
				Type:      drs.AccessMethodType("s3"),
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
	p := strings.TrimSpace(rawPath)
	if p == "" {
		return "", nil
	}
	if !strings.HasPrefix(strings.ToLower(p), config.S3Prefix) {
		return "", fmt.Errorf("path must use %s<bucket>/<prefix> format", config.S3Prefix)
	}
	u, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("invalid s3 path: %w", err)
	}
	if !strings.EqualFold(u.Scheme, "s3") || strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("invalid s3 path")
	}
	if !strings.EqualFold(strings.TrimSpace(u.Host), strings.TrimSpace(bucket)) {
		return "", fmt.Errorf("s3 path bucket does not match bucket")
	}
	return strings.Trim(strings.TrimSpace(u.Path), "/"), nil
}

func objectURLForCredential(cred *core.S3Credential, key string) (string, error) {
	if cred == nil {
		return "", fmt.Errorf("credential is required")
	}
	cleanKey := strings.TrimPrefix(strings.TrimSpace(key), "/")
	p := providerForCredential(cred)
	switch p {
	case provider.S3:
		return fmt.Sprintf("%s%s/%s", config.S3Prefix, cred.Bucket, cleanKey), nil
	case provider.GCS:
		return fmt.Sprintf("%s%s/%s", config.GCSPrefix, cred.Bucket, cleanKey), nil
	case provider.Azure:
		return fmt.Sprintf("%s%s/%s", config.AzurePrefix, cred.Bucket, cleanKey), nil
	case provider.File:
		root := strings.TrimSpace(cred.Endpoint)
		if root != "" {
			root = strings.TrimSuffix(root, "/")
			return fmt.Sprintf("%s/%s", root, cleanKey), nil
		}
		return fmt.Sprintf("%s%s/%s", config.FilePrefix, strings.TrimPrefix(cred.Bucket, "/"), cleanKey), nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", p)
	}
}

// --- Internal Validation and Normalization Helpers ---

func providerForCredential(cred *core.S3Credential) string {
	if cred == nil {
		return provider.S3
	}
	return provider.Normalize(cred.Provider, provider.S3)
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
	hashType := normalizeHashQueryType(rawType)
	hashValue := normalizeHashQueryValue(rawHash)

	cleanHash := strings.Trim(strings.TrimSpace(rawHash), `"'`)
	if hashType == "" {
		if parts := strings.SplitN(cleanHash, ":", 2); len(parts) == 2 {
			hashType = normalizeHashQueryType(parts[0])
		}
	}
	return hashType, hashValue
}

func objectHasChecksumTypeAndValue(obj core.InternalObject, hashType string, hashValue string) bool {
	targetType := normalizeHashQueryType(hashType)
	targetValue := normalizeHashQueryValue(hashValue)
	if targetType == "" || targetValue == "" {
		return false
	}
	for _, cs := range obj.Checksums {
		if normalizeHashQueryType(cs.Type) != targetType {
			continue
		}
		if normalizeHashQueryValue(cs.Checksum) == targetValue {
			return true
		}
	}
	return false
}

func looksLikeSHA256(v string) bool {
	s := strings.TrimSpace(strings.ToLower(v))
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func checksumHintFromInputs(guid, fileName string) string {
	g := strings.TrimSpace(guid)
	if looksLikeSHA256(g) {
		return g
	}
	f := strings.TrimSpace(fileName)
	if looksLikeSHA256(f) {
		return f
	}
	parts := strings.Split(strings.Trim(f, "/"), "/")
	if len(parts) > 0 {
		last := strings.TrimSpace(parts[len(parts)-1])
		if looksLikeSHA256(last) {
			return last
		}
	}
	return ""
}

func targetResourcesFromObject(obj *core.InternalObject) []string {
	if obj == nil || len(obj.Authorizations) == 0 {
		return nil
	}
	return append([]string(nil), obj.Authorizations...)
}

func firstAuthz(authz []string) string {
	if len(authz) == 0 {
		return ""
	}
	return authz[0]
}
