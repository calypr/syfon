package drs

import (
	"time"

	drsapi "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
)

func drsObjectToSyfonInternalRecord(obj *DRSObject) (*internalapi.InternalRecord, error) {
	if obj == nil {
		return nil, nil
	}
	var ams []AccessMethod
	if obj.AccessMethods != nil {
		ams = *obj.AccessMethods
	}
	out := &internalapi.InternalRecord{
		Did:   obj.Id,
		Authz: InternalAuthzFromDrsAccessMethods(ams),
		Size:  Ptr(obj.Size),
		Urls:  Ptr(InternalURLFromDrsAccessURLs(ams)),
	}
	if obj.Name != nil && *obj.Name != "" {
		out.FileName = obj.Name
	}
	hashes := internalapi.HashInfo(convertDrsChecksumsToMap(obj.Checksums))
	out.Hashes = &hashes

	if obj.Version != nil && *obj.Version != "" {
		out.Version = obj.Version
	}
	if obj.Description != nil && *obj.Description != "" {
		out.Description = obj.Description
	}
	if !obj.CreatedTime.IsZero() {
		out.CreatedTime = Ptr(obj.CreatedTime.Format(time.RFC3339))
	}
	if obj.UpdatedTime != nil && !obj.UpdatedTime.IsZero() {
		out.UpdatedTime = Ptr(obj.UpdatedTime.Format(time.RFC3339))
	}
	return out, nil
}

func syfonInternalRecordToDRSObjectFromRecord(rec internalapi.InternalRecord) (*DRSObject, error) {
	resp := internalapi.InternalRecordResponse{
		Did:          rec.Did,
		Hashes:       rec.Hashes,
		Size:         rec.Size,
		Urls:         rec.Urls,
		Authz:        rec.Authz,
		FileName:     rec.FileName,
		Organization: rec.Organization,
		Project:      rec.Project,
	}
	return syfonInternalRecordToDRSObject(resp)
}

func syfonInternalRecordToDRSObject(rec internalapi.InternalRecordResponse) (*DRSObject, error) {
	var urls []string
	if rec.Urls != nil {
		urls = *rec.Urls
	}
	accessMethods, err := DRSAccessMethodsFromInternalURLs(urls, rec.Authz)
	if err != nil {
		return nil, err
	}
	var hashes map[string]string
	if rec.Hashes != nil {
		hashes = map[string]string(*rec.Hashes)
	}
	checksums := convertMapToDrsChecksums(hashes)
	did := rec.Did
	obj := &DRSObject{
		Id:            did,
		SelfUri:       "drs://" + did,
		AccessMethods: &accessMethods,
		Checksums:     checksums,
	}
	if rec.Size != nil {
		obj.Size = *rec.Size
	}
	if rec.FileName != nil && *rec.FileName != "" {
		obj.Name = rec.FileName
	}
	if rec.Version != nil && *rec.Version != "" {
		obj.Version = rec.Version
	}
	if rec.Description != nil && *rec.Description != "" {
		obj.Description = rec.Description
	}
	
	createdTimeStr := ""
	if rec.CreatedTime != nil {
		createdTimeStr = *rec.CreatedTime
	} else if rec.CreatedDate != nil {
		createdTimeStr = *rec.CreatedDate
	}
	if t, ok := parseRFC3339(createdTimeStr); ok {
		obj.CreatedTime = t
	}

	updatedTimeStr := ""
	if rec.UpdatedTime != nil {
		updatedTimeStr = *rec.UpdatedTime
	} else if rec.UpdatedDate != nil {
		updatedTimeStr = *rec.UpdatedDate
	}
	if t, ok := parseRFC3339(updatedTimeStr); ok {
		obj.UpdatedTime = Ptr(t)
	}
	return obj, nil
}

func convertDrsChecksumsToMap(checksums []drsapi.Checksum) map[string]string {
	result := make(map[string]string, len(checksums))
	for _, c := range checksums {
		result[c.Type] = c.Checksum
	}
	return result
}

func convertMapToDrsChecksums(hashes map[string]string) []drsapi.Checksum {
	result := make([]drsapi.Checksum, 0, len(hashes))
	for t, c := range hashes {
		result = append(result, drsapi.Checksum{
			Type:     t,
			Checksum: c,
		})
	}
	return result
}

func parseRFC3339(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func Ptr[T any](v T) *T {
	return &v
}
