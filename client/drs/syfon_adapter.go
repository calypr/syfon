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
	out := internalapi.NewInternalRecord(obj.Id, InternalAuthzFromDrsAccessMethods(obj.AccessMethods))
	out.SetSize(obj.Size)
	out.SetUrls(InternalURLFromDrsAccessURLs(obj.AccessMethods))
	out.SetAuthz(InternalAuthzFromDrsAccessMethods(obj.AccessMethods))
	out.SetHashes(convertDrsChecksumsToMap(obj.Checksums))
	if obj.Version != "" {
		out.SetVersion(obj.Version)
	}
	if obj.Description != "" {
		out.SetDescription(obj.Description)
	}
	if !obj.CreatedTime.IsZero() {
		out.SetCreatedTime(obj.CreatedTime.Format(time.RFC3339))
	}
	if !obj.UpdatedTime.IsZero() {
		out.SetUpdatedTime(obj.UpdatedTime.Format(time.RFC3339))
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
	accessMethods, err := DRSAccessMethodsFromInternalURLs(rec.GetUrls(), rec.GetAuthz())
	if err != nil {
		return nil, err
	}
	checksums := convertMapToDrsChecksums(rec.GetHashes())
	did := rec.GetDid()
	obj := &DRSObject{
		Id:            did,
		SelfUri:       "drs://" + did,
		Size:          rec.GetSize(),
		AccessMethods: accessMethods,
		Checksums:     checksums,
	}
	if rec.GetFileName() != "" {
		obj.Name = rec.GetFileName()
	}
	if rec.GetVersion() != "" {
		obj.Version = rec.GetVersion()
	}
	if rec.GetDescription() != "" {
		obj.Description = rec.GetDescription()
	}
	if t, ok := parseRFC3339(rec.GetCreatedTime()); ok {
		obj.CreatedTime = t
	} else if t, ok := parseRFC3339(rec.GetCreatedDate()); ok {
		obj.CreatedTime = t
	}
	if t, ok := parseRFC3339(rec.GetUpdatedTime()); ok {
		obj.UpdatedTime = t
	} else if t, ok := parseRFC3339(rec.GetUpdatedDate()); ok {
		obj.UpdatedTime = t
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
