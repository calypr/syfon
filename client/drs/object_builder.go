package drs

import (
	"fmt"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/drs"
)

type ObjectBuilder struct {
	Bucket        string
	ProjectID     string
	Organization  string
	StoragePrefix string
	AccessType    string
	PathStyle     string // "CAS" or "" (Gen3 default)
}

func NewObjectBuilder(bucket, projectID string) ObjectBuilder {
	return ObjectBuilder{
		Bucket:     bucket,
		ProjectID:  projectID,
		AccessType: "s3",
		PathStyle:  "Gen3", // Defaults to Gen3 behavior
	}
}

func (b ObjectBuilder) Build(fileName string, checksum string, size int64, drsID string) (*DRSObject, error) {
	if b.Bucket == "" {
		return nil, fmt.Errorf("error: bucket name is empty in config file")
	}
	accessType := b.AccessType
	if accessType == "" {
		accessType = "s3"
	}

	// Remove sha256: prefix if present for clean S3 key.
	checksum = strings.TrimPrefix(checksum, "sha256:")
	prefix := strings.Trim(strings.TrimSpace(b.StoragePrefix), "/")
	if prefix == "" {
		prefix = StoragePrefix(b.Organization, b.ProjectID)
	}

	var fileURL string
	// Canonical CAS-style (s3://bucket/{org}/{project}/sha256).
	// PathStyle is kept for compatibility, but object identity is checksum-first.
	if prefix != "" {
		fileURL = fmt.Sprintf("s3://%s/%s/%s", b.Bucket, prefix, checksum)
	} else {
		fileURL = fmt.Sprintf("s3://%s/%s", b.Bucket, checksum)
	}

	authzStr, err := ProjectToResource(b.Organization, b.ProjectID)
	if err != nil {
		return nil, err
	}

	drsObj := DRSObject{
		Id:   drsID,
		Name: &fileName,
		AccessMethods: &[]AccessMethod{{
			Type: drsapi.AccessMethodType(accessType),
			AccessUrl: &struct {
				Headers *[]string "json:\"headers,omitempty\""
				Url     string    "json:\"url\""
			}{
				Url: fileURL,
			},
			Authorizations: &struct {
				BearerAuthIssuers   *[]string                                            "json:\"bearer_auth_issuers,omitempty\""
				DrsObjectId         *string                                              "json:\"drs_object_id,omitempty\""
				PassportAuthIssuers *[]string                                            "json:\"passport_auth_issuers,omitempty\""
				SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes "json:\"supported_types,omitempty\""
			}{
				BearerAuthIssuers: &[]string{authzStr},
			},
		}},
		Checksums: []Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
		Size: size,
	}

	return &drsObj, nil
}
