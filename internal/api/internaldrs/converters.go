package internaldrs

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/internalapi"
	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/internal/provider"
	"github.com/google/uuid"
)

// --- Domain Mapping Tools ---

func canonicalIDFromInternal(req internalapi.InternalRecord) string {
	if did := strings.TrimSpace(req.GetDid()); did != "" {
		if _, err := uuid.Parse(did); err == nil {
			return did
		}
	}
	hashes := req.GetHashes()
	if v, ok := hashes["sha256"]; ok && strings.TrimSpace(v) != "" {
		authz := append([]string(nil), req.GetAuthz()...)
		if len(authz) == 0 && req.HasOrganization() {
			path := core.ResourcePathForScope(req.GetOrganization(), req.GetProject())
			if path != "" {
				authz = append(authz, path)
			}
		}
		return core.MintObjectIDFromChecksum(v, authz)
	}
	return ""
}

func internalToDrs(req internalapi.InternalRecord) (*core.InternalObject, error) {
	id := canonicalIDFromInternal(req)
	if id == "" {
		return nil, fmt.Errorf("sha256 hash is required unless did is a UUID")
	}
	now := time.Now()
	obj := &drs.DrsObject{
		Id:          id,
		SelfUri:     config.DRSPrefix + id,
		Size:        req.GetSize(),
		CreatedTime: now,
		UpdatedTime: now,
		Name:        req.GetFileName(),
	}
	for t, v := range req.GetHashes() {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}
	if len(obj.Checksums) == 0 {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
	}
	for _, u := range req.GetUrls() {
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			Type:      "s3",
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Region:    config.DefaultS3Region,
		})
	}
	authz := append([]string(nil), req.GetAuthz()...)
	if len(authz) == 0 && req.HasOrganization() {
		path := core.ResourcePathForScope(req.GetOrganization(), req.GetProject())
		if path != "" {
			authz = append(authz, path)
		}
	}
	for i := range obj.AccessMethods {
		obj.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
			BearerAuthIssuers: authz,
		}
	}
	return &core.InternalObject{DrsObject: *obj, Authorizations: authz}, nil
}

func drsToInternal(obj *core.InternalObject) internalapi.InternalRecordResponse {
	hashes := make(map[string]string, len(obj.Checksums))
	for _, c := range obj.Checksums {
		hashes[c.Type] = c.Checksum
	}
	if len(hashes) == 0 && obj.Id != "" {
		hashes["sha256"] = obj.Id
	}

	var urls []string
	authz := append([]string(nil), obj.Authorizations...)
	if len(obj.AccessMethods) > 0 {
		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url != "" {
				urls = append(urls, am.AccessUrl.Url)
			}
		}
	}
	scope := core.ParseResourcePath(firstAuthz(authz))
	resp := internalapi.InternalRecordResponse{
		Authz: authz,
		Urls:  urls,
	}
	if obj.Id != "" {
		resp.SetDid(obj.Id)
	}
	resp.SetSize(obj.Size)
	if len(hashes) > 0 {
		resp.SetHashes(hashes)
	}
	if obj.Name != "" {
		resp.SetFileName(obj.Name)
	}
	if scope.Organization != "" {
		resp.SetOrganization(scope.Organization)
	}
	if scope.Project != "" {
		resp.SetProject(scope.Project)
	}
	if !obj.CreatedTime.IsZero() {
		resp.SetCreatedDate(obj.CreatedTime.Format(time.RFC3339))
	}
	if !obj.UpdatedTime.IsZero() {
		resp.SetUpdatedDate(obj.UpdatedTime.Format(time.RFC3339))
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
