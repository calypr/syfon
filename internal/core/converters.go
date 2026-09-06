package core

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/objects"
)

func EnforceCanonicalProjectScope(obj objects.Record, organization, project string) (objects.Record, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if project != "" && organization == "" {
		return objects.Record{}, fmt.Errorf("organization is required when project is set")
	}
	if organization == "" || project == "" {
		return obj, nil
	}

	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return objects.Record{}, err
	}
	controlled := append(ObjectAccessResources(&obj), resource)
	controlled = syfoncommon.NormalizeAccessResources(controlled)
	obj.ControlledAccess = &controlled
	obj.Authorizations = syfoncommon.ControlledAccessToAuthzMap(controlled)
	return obj, nil
}

// FirstSupportedAccessURL returns the first URL from an object that Syfon can sign.
func FirstSupportedAccessURL(obj *objects.Record) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		scheme := common.SchemeFromURL(am.AccessUrl.Url)
		if scheme != "" && common.ProviderFromScheme(scheme) == "" {
			continue
		}
		return am.AccessUrl.Url
	}
	return ""
}

// CandidateToRecord converts a domain registration candidate to a persisted record.
func CandidateToRecord(c objects.Candidate, now time.Time) (objects.Record, error) {
	checksums := append([]objects.Checksum(nil), candidateChecksums(c.Checksums)...)
	oid, ok := objects.CanonicalSHA256(checksums)
	if !ok {
		return objects.Record{}, objects.ErrNoValidSHA256
	}
	if c.AccessMethods == nil || len(*c.AccessMethods) == 0 {
		return objects.Record{}, objects.ErrAccessMethodsRequired
	}
	authzList := syfoncommon.ControlledAccessToAuthzMap(common.DerefStringSlice(c.ControlledAccess))

	id := ""
	if c.Aliases != nil {
		for _, a := range *c.Aliases {
			if strings.HasPrefix(a, "id:") {
				id = strings.TrimPrefix(a, "id:")
				break
			}
		}
	}

	if id == "" {
		mintedID, mintErr := objects.MintRecordIDFromChecksum(oid, syfoncommon.AuthzMapToList(authzList))
		if mintErr != nil {
			return objects.Record{}, mintErr
		}
		id = string(mintedID)
	}

	obj := objects.Record{
		Id:          objects.RecordID(id),
		Size:        common.Int64Val(c.Size),
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     common.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     c.Aliases,
		Checksums:   []objects.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if c.ControlledAccess != nil {
		controlled := syfoncommon.NormalizeAccessResources(*c.ControlledAccess)
		obj.ControlledAccess = &controlled
	}
	if c.Name != nil {
		obj.Name = normalizedObjectNamePtr(c.Name)
	}
	if obj.Name == nil || strings.TrimSpace(*obj.Name) == "" {
		obj.Name = &oid
	}
	obj.SelfUri = "drs://" + string(obj.Id)

	// Re-construct access methods with clean IDs
	if c.AccessMethods != nil {
		newMethods := make([]objects.AccessMethod, 0, len(*c.AccessMethods))
		for _, method := range *c.AccessMethods {
			if method.AccessId == nil || *method.AccessId == "" {
				method.AccessId = common.Ptr(method.Type)
			}
			newMethods = append(newMethods, method)
		}
		obj.AccessMethods = &newMethods
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) == 0 {
		return objects.Record{}, objects.ErrAccessMethodsRequired
	}

	obj.Authorizations = authzList
	return obj, nil
}

func candidateChecksums(value *[]objects.Checksum) []objects.Checksum {
	if value == nil {
		return nil
	}
	return *value
}

// MergeRecordUpdate merges an update into an existing object.
func MergeRecordUpdate(existing objects.Record, update objects.Record, id string, now time.Time) (objects.Record, error) {
	merged := existing
	merged.Id = objects.RecordID(id)
	merged.UpdatedTime = &now
	if update.Properties != nil {
		if merged.Properties == nil {
			merged.Properties = make(map[string]json.RawMessage, len(update.Properties))
		}
		for k, v := range update.Properties {
			merged.Properties[k] = v
		}
	}

	if update.Name != nil {
		merged.Name = normalizedObjectNamePtr(update.Name)
	}
	if update.Description != nil {
		merged.Description = update.Description
	}
	if update.MimeType != nil {
		merged.MimeType = update.MimeType
	}
	if update.Version != nil {
		merged.Version = update.Version
	}
	if update.Aliases != nil {
		merged.Aliases = update.Aliases
	}
	if update.Authorizations != nil {
		merged.Authorizations = update.Authorizations
	}
	if update.ControlledAccess != nil {
		merged.ControlledAccess = update.ControlledAccess
		merged.Authorizations = syfoncommon.ControlledAccessToAuthzMap(*update.ControlledAccess)
	}
	if update.AccessMethods != nil {
		merged.AccessMethods = update.AccessMethods
	}
	if update.Checksums != nil {
		merged.Checksums = objects.MergeAdditionalChecksums(existing.Checksums, update.Checksums)
	}

	return merged, nil
}

func normalizedObjectNamePtr(name *string) *string {
	if name == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*name)
	if trimmed == "" {
		return nil
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	base := filepath.Base(trimmed)
	if base == "." || base == "/" || base == "" {
		base = trimmed
	}
	return common.Ptr(base)
}
