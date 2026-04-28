package core

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/apigen/server/lfsapi"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

// UniqueAuthz merges authorizations from all access methods into a single map.
func UniqueAuthz(accessMethods []drs.AccessMethod) map[string][]string {
	if len(accessMethods) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for _, am := range accessMethods {
		if am.Authorizations == nil {
			continue
		}
		for org, projects := range *am.Authorizations {
			if len(projects) == 0 {
				if _, ok := out[org]; !ok {
					out[org] = []string{}
				}
				continue
			}
			seen := make(map[string]struct{}, len(out[org]))
			for _, p := range out[org] {
				seen[p] = struct{}{}
			}
			for _, p := range projects {
				if _, ok := seen[p]; !ok {
					out[org] = append(out[org], p)
					seen[p] = struct{}{}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// LFSCandidateToDRS converts an LFS-specific candidate to a DRS-generic one.
func LFSCandidateToDRS(in lfsapi.DrsObjectCandidate) drs.DrsObjectCandidate {
	var checksums []drs.Checksum
	if in.Checksums != nil {
		checksums = make([]drs.Checksum, len(*in.Checksums))
		for i, c := range *in.Checksums {
			checksums[i] = drs.Checksum{Type: c.Type, Checksum: c.Checksum}
		}
	}

	var ams *[]drs.AccessMethod
	if in.AccessMethods != nil {
		converted := make([]drs.AccessMethod, len(*in.AccessMethods))
		for i, am := range *in.AccessMethods {
			var accessURL *struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    "json:\"url\""
			}
			if am.AccessUrl != nil && am.AccessUrl.Url != nil {
				accessURL = &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    "json:\"url\""
				}{Url: *am.AccessUrl.Url}
			}

			converted[i] = drs.AccessMethod{
				AccessId:  am.AccessId,
				AccessUrl: accessURL,
				Cloud:     am.Region,
			}
			if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
				if authzMap := syfoncommon.AuthzListToMap(*am.Authorizations.BearerAuthIssuers); authzMap != nil {
					converted[i].Authorizations = &authzMap
				}
			}
			if am.Type != nil {
				converted[i].Type = drs.AccessMethodType(*am.Type)
			}
		}
		ams = &converted
	}

	return drs.DrsObjectCandidate{
		Name:          in.Name,
		Size:          *in.Size,
		MimeType:      in.MimeType,
		Description:   in.Description,
		Aliases:       in.Aliases,
		Checksums:     checksums,
		AccessMethods: ams,
	}
}

// FirstSupportedAccessURL returns the first URL from an object that Syfon can sign.
func FirstSupportedAccessURL(obj *models.InternalObject) string {
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

// S3KeyFromInternalObjectForBucket extracts a key for a specific bucket from an object.
func S3KeyFromInternalObjectForBucket(obj *models.InternalObject, bucket string) (string, bool) {
	if obj == nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	if targetBucket == "" {
		return "", false
	}
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if b, key, ok := common.ParseS3URL(raw); ok && strings.EqualFold(b, targetBucket) {
				return key, true
			}
		}
	}
	return "", false
}

// CandidateToInternalObject converts a DRS registration candidate to our internal domain model.
func CandidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (models.InternalObject, error) {
	oid, ok := common.CanonicalSHA256(c.Checksums)
	if !ok {
		return models.InternalObject{}, common.ErrNoValidSHA256
	}
	var ams []drs.AccessMethod
	if c.AccessMethods != nil {
		ams = *c.AccessMethods
	}
	authzList := UniqueAuthz(ams)

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
		id = common.MintObjectIDFromChecksum(oid, syfoncommon.AuthzMapToList(authzList))
	}

	obj := drs.DrsObject{
		Id:          id,
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     common.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     c.Aliases,
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if c.Name != nil {
		obj.Name = c.Name
	}
	if obj.Name == nil || strings.TrimSpace(*obj.Name) == "" {
		obj.Name = &oid
	}
	obj.SelfUri = "drs://" + obj.Id

	// Re-construct access methods with clean IDs
	if c.AccessMethods != nil {
		newMethods := make([]drs.AccessMethod, 0, len(*c.AccessMethods))
		for _, am := range *c.AccessMethods {
			method := am
			if method.AccessId == nil || *method.AccessId == "" {
				method.AccessId = common.Ptr(string(method.Type))
			}
			if method.Authorizations == nil && len(authzList) > 0 {
				method.Authorizations = &authzList
			}
			newMethods = append(newMethods, method)
		}
		obj.AccessMethods = &newMethods
	}

	return models.InternalObject{
		DrsObject:      obj,
		Authorizations: authzList,
	}, nil
}

// MergeInternalObjectUpdate merges an update into an existing object.
func MergeInternalObjectUpdate(existing models.InternalObject, update models.InternalObject, id string, now time.Time) (models.InternalObject, error) {
	merged := existing
	merged.DrsObject.Id = id
	merged.DrsObject.UpdatedTime = &now

	if update.DrsObject.Name != nil {
		merged.DrsObject.Name = update.DrsObject.Name
	}
	if update.DrsObject.Description != nil {
		merged.DrsObject.Description = update.DrsObject.Description
	}
	if update.DrsObject.MimeType != nil {
		merged.DrsObject.MimeType = update.DrsObject.MimeType
	}
	if update.DrsObject.Version != nil {
		merged.DrsObject.Version = update.DrsObject.Version
	}
	if update.Aliases != nil {
		merged.Aliases = update.Aliases
	}
	if update.Authorizations != nil {
		merged.Authorizations = update.Authorizations
	}
	if update.AccessMethods != nil {
		merged.AccessMethods = update.AccessMethods
	}
	if update.Checksums != nil {
		merged.Checksums = common.MergeAdditionalChecksums(existing.Checksums, update.Checksums)
	}

	return merged, nil
}

// InternalRecordToInternalObject converts an index/internal record to our internal domain model.
func InternalRecordToInternalObject(r internalapi.InternalRecord, now time.Time) (models.InternalObject, error) {
	id := strings.TrimSpace(r.Did)
	if id == "" {
		return models.InternalObject{}, fmt.Errorf("did is required")
	}

	obj := drs.DrsObject{
		Id:          id,
		Size:        common.Int64Val(r.Size),
		CreatedTime: parseInternalRecordTime(r.CreatedTime, now),
		Version:     common.Ptr("1"),
		Description: r.Description,
	}
	updatedTime := parseInternalRecordTime(r.UpdatedTime, obj.CreatedTime)
	obj.UpdatedTime = &updatedTime
	if r.FileName != nil {
		obj.Name = r.FileName
	}
	if v := r.Version; v != nil {
		obj.Version = v
	}

	if r.Hashes != nil {
		checksums := make([]drs.Checksum, 0, len(*r.Hashes))
		for k, v := range *r.Hashes {
			checksums = append(checksums, drs.Checksum{Type: k, Checksum: v})
		}
		obj.Checksums = checksums
	}

	var auth models.AuthPathMap
	var authzMap map[string][]string
	if r.Auth != nil {
		auth = models.AuthPathMap(*r.Auth)
		authzMap = models.AuthPathMapToAuthorizations(auth)
		methods := AccessMethodsFromAuthPathMap(auth)
		if len(methods) > 0 {
			obj.AccessMethods = &methods
		}
	}
	return models.InternalObject{
		DrsObject:      obj,
		Auth:           auth,
		Authorizations: authzMap,
	}, nil
}

func parseInternalRecordTime(raw *string, fallback time.Time) time.Time {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return fallback.UTC()
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999", "2006-01-02 15:04:05.999999", "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(*raw)); err == nil {
			return parsed.UTC()
		}
	}
	return fallback.UTC()
}

// InternalObjectToInternalRecord converts our internal domain model back to an API record.
func InternalObjectToInternalRecord(obj models.InternalObject) internalapi.InternalRecord {
	var authPtr *internalapi.AuthPathMap
	if len(obj.Auth) > 0 {
		auth := internalapi.AuthPathMap(obj.Auth)
		authPtr = &auth
	}
	res := internalapi.InternalRecord{
		Did:         obj.Id,
		Size:        &obj.Size,
		CreatedTime: common.Ptr(obj.CreatedTime.Format(time.RFC3339)),
		Description: obj.Description,
		FileName:    obj.Name,
		Version:     obj.Version,
		Auth:        authPtr,
	}
	if obj.UpdatedTime != nil {
		res.UpdatedTime = common.Ptr(obj.UpdatedTime.Format(time.RFC3339))
	}
	if len(obj.Checksums) > 0 {
		h := make(internalapi.HashInfo)
		for _, c := range obj.Checksums {
			h[c.Type] = c.Checksum
		}
		res.Hashes = &h
	}
	return res
}

// InternalObjectToInternalRecordResponse converts our internal domain model back to an API response.
func InternalObjectToInternalRecordResponse(obj models.InternalObject) internalapi.InternalRecordResponse {
	rec := InternalObjectToInternalRecord(obj)
	return internalapi.InternalRecordResponse{
		Did:          rec.Did,
		Size:         rec.Size,
		CreatedTime:  rec.CreatedTime,
		Description:  rec.Description,
		FileName:     rec.FileName,
		Version:      rec.Version,
		Auth:         rec.Auth,
		UpdatedTime:  rec.UpdatedTime,
		Hashes:       rec.Hashes,
		Organization: rec.Organization,
		Project:      rec.Project,
	}
}

func AccessMethodsFromAuthPathMap(auth models.AuthPathMap) []drs.AccessMethod {
	methods := make([]drs.AccessMethod, 0)
	for org, projects := range auth {
		for project, paths := range projects {
			methodAuthz := map[string][]string{org: {project}}
			if project == "" {
				methodAuthz[org] = []string{}
			}
			for _, rawPath := range paths {
				rawPath = strings.TrimSpace(rawPath)
				if rawPath == "" {
					continue
				}
				scheme := "https"
				if parsed, err := url.Parse(rawPath); err == nil && parsed.Scheme != "" {
					scheme = parsed.Scheme
				}
				methods = append(methods, drs.AccessMethod{
					Type:     drs.AccessMethodType(scheme),
					AccessId: common.Ptr(scheme),
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: rawPath},
					Authorizations: &methodAuthz,
				})
			}
		}
	}
	return methods
}
