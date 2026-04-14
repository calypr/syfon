package drs

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// NAMESPACE is the UUID namespace used for generating DRS UUIDs
var NAMESPACE = uuid.NewMD5(uuid.NameSpaceURL, []byte("calypr.org"))

func sanitizePathComponent(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "/")
	v = strings.ReplaceAll(v, "\\", "/")
	return strings.ReplaceAll(v, " ", "_")
}

// StoragePrefix returns the bucket key prefix used for object placement.
// Preferred layout is "<organization>/<project>" when organization is provided.
// When organization is empty, it falls back to "<program>/<project>" for hyphenated
// project IDs or "default/<project>" otherwise.
func StoragePrefix(org, project string) string {
	org = sanitizePathComponent(org)
	project = sanitizePathComponent(project)
	if project == "" {
		return ""
	}
	if org != "" {
		return org + "/" + project
	}
	if strings.Contains(project, "-") {
		parts := strings.SplitN(project, "-", 2)
		return sanitizePathComponent(parts[0]) + "/" + sanitizePathComponent(parts[1])
	}
	return "default/" + project
}

func ProjectToResource(org, project string) (string, error) {
	if org != "" {
		return "/programs/" + org + "/projects/" + project, nil
	}
	if project == "" {
		return "", fmt.Errorf("error: project ID is empty")
	}
	if !strings.Contains(project, "-") {
		return "/programs/default/projects/" + project, nil
	}
	projectIdArr := strings.SplitN(project, "-", 2)
	return "/programs/" + projectIdArr[0] + "/projects/" + projectIdArr[1], nil
}

// From git-drs/drsmap/drs_map.go

// DrsUUID generates a deterministic version 5 UUID for a DRS object
// based on its scope (organization and project) and content hash.
func DrsUUID(org, project, hash string) string {
	// 1. Normalize hash - strip sha256: prefix if present
	hash = NormalizeOid(hash)

	// 2. Resolve canonical resource path for the project.
	// This ensures that same project names in different organizations produce different IDs.
	resource, err := ProjectToResource(org, project)
	if err != nil {
		// Fallback to simple concatenation if project info is corrupt
		resource = org + ":" + project
	}

	// 3. Create UUID based on resource path and hash
	// We use the canonical "resource:hash" string as the name for the V5 UUID.
	seed := fmt.Sprintf("%s:%s", resource, hash)
	return uuid.NewSHA1(NAMESPACE, []byte(seed)).String()
}

func FindMatchingRecord(records []DRSObject, organization, projectId string) (*DRSObject, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Convert project ID to resource path format for comparison
	expectedAuthz, err := ProjectToResource(organization, projectId)
	if err != nil {
		return nil, fmt.Errorf("error converting project ID to resource format: %v", err)
	}

	for _, record := range records {
		for _, access := range record.AccessMethods {
			if len(access.Authorizations.BearerAuthIssuers) == 0 {
				continue
			}

			// Check BearerAuthIssuers using a map for O(1) lookup (ref: "lists suck")
			issuersMap := make(map[string]struct{}, len(access.Authorizations.BearerAuthIssuers))
			for _, issuer := range access.Authorizations.BearerAuthIssuers {
				issuersMap[issuer] = struct{}{}
			}

			if _, ok := issuersMap[expectedAuthz]; ok {
				return &record, nil
			}
		}
	}
	return nil, nil
}

// DRS UUID generation using SHA1 (compatible with git-drs)
func GenerateDrsID(org, project, hash string) string {
	return DrsUUID(org, project, hash)
}

func BuildDrsObj(fileName string, checksum string, size int64, drsId string, bucketName string, org string, projectId string) (*DRSObject, error) {
	return BuildDrsObjWithPrefix(fileName, checksum, size, drsId, bucketName, org, projectId, "")
}

func BuildDrsObjWithPrefix(fileName string, checksum string, size int64, drsId string, bucketName string, org string, projectId string, storagePrefix string) (*DRSObject, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("error: bucket name is empty")
	}

	checksum = NormalizeOid(checksum)
	prefix := strings.Trim(strings.TrimSpace(storagePrefix), "/")
	if prefix == "" {
		prefix = StoragePrefix(org, projectId)
	}
	var fileURL string
	// Canonical CAS-style storage path:
	// s3://bucket/{org}/{project}/sha256
	if prefix != "" {
		fileURL = fmt.Sprintf("s3://%s/%s/%s", bucketName, prefix, checksum)
	} else {
		fileURL = fmt.Sprintf("s3://%s/%s", bucketName, checksum)
	}

	authzStr, err := ProjectToResource(org, projectId)
	if err != nil {
		return nil, err
	}
	authorizations := Authorizations{
		BearerAuthIssuers: []string{authzStr},
	}

	drsObj := DRSObject{
		Id:   drsId,
		Name: fileName,
		AccessMethods: []AccessMethod{{
			Type: "s3",
			AccessUrl: AccessURL{
				Url: fileURL,
			},
			Authorizations: authorizations,
		}},
		Checksums: []Checksum{{
			Type: "sha256",
			Checksum: checksum,
		}},
		Size:      size,
	}

	return &drsObj, nil
}

// ConvertToCandidate converts a DRSObject to a DRSObjectCandidate for registration.
// We use manual assignment to resolve deep alias walls and satisfy compiler strictness.
func ConvertToCandidate(obj *DRSObject) DRSObjectCandidate {
	if obj == nil {
		return DRSObjectCandidate{}
	}

	return DRSObjectCandidate{
		Name:          obj.Name,
		Size:          obj.Size,
		Checksums:     obj.Checksums,
		AccessMethods: obj.AccessMethods,
		Contents:      obj.Contents,
		Aliases:       obj.Aliases,
		Description:   obj.Description,
		Version:       obj.Version,
		MimeType:      obj.MimeType,
	}
}

func NormalizeOid(oid string) string {
	if strings.HasPrefix(oid, "sha256:") {
		return strings.TrimPrefix(oid, "sha256:")
	}
	return oid
}
