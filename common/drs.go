package common

import (
	"fmt"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/google/uuid"
)

// drsUUIDNamespace is a fixed namespace UUID for deterministic DRS ID generation.
var drsUUIDNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

// DrsUUID generates a deterministic UUID for a DRS object from org, project, and hash.
func DrsUUID(org, project, hash string) string {
	name := org + "/" + project + "/" + hash
	return uuid.NewSHA1(drsUUIDNamespace, []byte(name)).String()
}

// ObjectBuilder constructs DRS objects for a given bucket and project scope.
type ObjectBuilder struct {
	Bucket        string
	ProjectID     string
	Organization  string
	StoragePrefix string
	PathStyle     string
}

// NewObjectBuilder creates an ObjectBuilder for the given bucket and project.
func NewObjectBuilder(bucket, projectId string) ObjectBuilder {
	return ObjectBuilder{
		Bucket:    bucket,
		ProjectID: projectId,
	}
}

// Build constructs a DRS object with the given file metadata.
func (b ObjectBuilder) Build(fileName string, checksum string, size int64, drsId string) (*drsapi.DrsObject, error) {
	prefix := b.StoragePrefix
	if prefix == "" {
		prefix = StoragePrefix(b.Organization, b.ProjectID)
	}
	return BuildDrsObjWithPrefix(fileName, checksum, size, drsId, b.Bucket, b.Organization, b.ProjectID, prefix)
}

// BuildDrsObjWithPrefix builds a DRS object with an S3 access URL derived from
// the provided bucket, org, project, and storage prefix.
func BuildDrsObjWithPrefix(fileName string, checksum string, size int64, drsId string, bucket string, org string, projectId string, prefix string) (*drsapi.DrsObject, error) {
	if checksum == "" {
		return nil, fmt.Errorf("checksum is required")
	}

	resourcePath, err := ResourcePath(org, projectId)
	if err != nil {
		return nil, err
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

	if resourcePath != "" {
		issuers := []string{resourcePath}
		am.Authorizations = &struct {
			BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
			DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
			PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
			SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
		}{
			BearerAuthIssuers: &issuers,
		}
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
		AccessMethods: obj.AccessMethods,
		Aliases:       obj.Aliases,
		Checksums:     obj.Checksums,
		Contents:      obj.Contents,
		Description:   obj.Description,
		MimeType:      obj.MimeType,
		Name:          obj.Name,
		Size:          obj.Size,
		Version:       obj.Version,
	}
}
