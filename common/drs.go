package common

import (
	"fmt"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/google/uuid"
)

// drsUUIDNamespace is a fixed namespace UUID for deterministic DRS ID generation.
var drsUUIDNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

type AccessMethodAuthorizations = struct {
	BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
	DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
	PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
	SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
}

// DrsUUID generates a deterministic UUID for a DRS object from org, project, and hash.
func DrsUUID(org, project, hash string) string {
	hash = NormalizeOid(hash)
	resource, err := ResourcePath(org, project)
	if err != nil || hash == "" || project == "" || resource == "" {
		return ""
	}
	name := "sha256:" + hash + "|" + resource
	return uuid.NewSHA1(drsUUIDNamespace, []byte(name)).String()
}

// ObjectBuilder constructs DRS objects for a given bucket and project scope.
type ObjectBuilder struct {
	Bucket        string
	Project       string
	Organization  string
	StoragePrefix string
	PathStyle     string
}

// NewObjectBuilder creates an ObjectBuilder for the given bucket and project.
func NewObjectBuilder(bucket, project string) ObjectBuilder {
	return ObjectBuilder{
		Bucket:  bucket,
		Project: project,
	}
}

// Build constructs a DRS object with the given file metadata.
func (b ObjectBuilder) Build(fileName string, checksum string, size int64, drsId string) (*drsapi.DrsObject, error) {
	prefix := b.StoragePrefix
	return BuildDrsObjWithPrefix(fileName, checksum, size, drsId, b.Bucket, b.Organization, b.Project, prefix)
}

// BuildDrsObjWithPrefix builds a DRS object with an S3 access URL derived from
// the provided bucket, org, project, and storage prefix.
func BuildDrsObjWithPrefix(fileName string, checksum string, size int64, drsId string, bucket string, org string, project string, prefix string) (*drsapi.DrsObject, error) {
	if checksum == "" {
		return nil, fmt.Errorf("checksum is required")
	}

	obj := &drsapi.DrsObject{
		Id:      drsId,
		SelfUri: "drs://" + drsId,
		Size:    size,
		Name:    &fileName,
		Checksums: []drsapi.Checksum{
			{Type: "sha256", Checksum: checksum},
		},
	}

	if bucket == "" {
		return obj, nil
	}

	var accessURL string
	if prefix != "" {
		accessURL = fmt.Sprintf("s3://%s/%s/%s", bucket, prefix, checksum)
	} else {
		accessURL = fmt.Sprintf("s3://%s/%s", bucket, checksum)
	}

	am := drsapi.AccessMethod{
		Type: drsapi.AccessMethodTypeS3,
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: accessURL},
	}

	if authzMap := AuthzMapFromScope(org, project); authzMap != nil {
		controlled := AuthzMapToControlledAccess(authzMap)
		obj.ControlledAccess = &controlled
	}

	ams := []drsapi.AccessMethod{am}
	obj.AccessMethods = &ams
	return obj, nil
}

// ConvertToCandidate converts a DRS object to a registration candidate,
// stripping server-assigned fields (Id, SelfUri, timestamps).
func ConvertToCandidate(obj *drsapi.DrsObject) drsapi.DrsObjectCandidate {
	if obj == nil {
		return drsapi.DrsObjectCandidate{}
	}
	return drsapi.DrsObjectCandidate{
		AccessMethods:    obj.AccessMethods,
		Aliases:          obj.Aliases,
		Checksums:        obj.Checksums,
		Contents:         obj.Contents,
		ControlledAccess: obj.ControlledAccess,
		Description:      obj.Description,
		MimeType:         obj.MimeType,
		Name:             obj.Name,
		Size:             obj.Size,
		Version:          obj.Version,
	}
}

func AccessMethodAuthorizationsFromAuthzMap(authzMap map[string][]string) *AccessMethodAuthorizations {
	if len(authzMap) == 0 {
		return nil
	}
	authzList := AuthzMapToList(authzMap)
	return &AccessMethodAuthorizations{BearerAuthIssuers: &authzList}
}

func AuthzMapFromAccessMethodAuthorizations(authz *AccessMethodAuthorizations) map[string][]string {
	if authz == nil || authz.BearerAuthIssuers == nil {
		return nil
	}
	return AuthzListToMap(*authz.BearerAuthIssuers)
}

func AccessMethodMatchesScope(method *drsapi.AccessMethod, org, project string) bool {
	if method == nil || method.Authorizations == nil {
		return false
	}
	return AuthzMapMatchesScope(AuthzMapFromAccessMethodAuthorizations(method.Authorizations), org, project)
}

func DrsObjectMatchesScope(obj *drsapi.DrsObject, org, project string) bool {
	if obj == nil {
		return false
	}
	if AuthzMapMatchesScope(ControlledAccessToAuthzMap(derefStringSlice(obj.ControlledAccess)), org, project) {
		return true
	}
	if obj.AccessMethods == nil {
		return false
	}
	for i := range *obj.AccessMethods {
		if AccessMethodMatchesScope(&(*obj.AccessMethods)[i], org, project) {
			return true
		}
	}
	return false
}

// EnsureAccessMethodAuthorizations adds authz to access methods that do not
// already define any bearer issuer scope. Existing authz and unrelated fields
// are preserved.
func EnsureAccessMethodAuthorizations(obj *drsapi.DrsObject, authzMap map[string][]string) (*drsapi.DrsObject, bool) {
	if obj == nil || obj.AccessMethods == nil || len(*obj.AccessMethods) == 0 || len(authzMap) == 0 {
		return obj, false
	}
	changed := false
	for i := range *obj.AccessMethods {
		method := &(*obj.AccessMethods)[i]
		if AuthzMapFromAccessMethodAuthorizations(method.Authorizations) != nil {
			continue
		}
		method.Authorizations = AccessMethodAuthorizationsFromAuthzMap(authzMap)
		changed = true
	}
	return obj, changed
}

func derefStringSlice(ptr *[]string) []string {
	if ptr == nil {
		return nil
	}
	return append([]string(nil), (*ptr)...)
}
