package drs

import (
	"time"

	syclient "github.com/calypr/syfon/client"
)

func drsObjectToSyfonInternalRecord(obj *DRSObject) (syclient.InternalRecord, error) {
	if obj == nil {
		return syclient.InternalRecord{}, nil
	}
	out := syclient.InternalRecord{}
	out.SetDid(obj.Id)
	if obj.Name != "" {
		out.SetFileName(obj.Name)
	}
	out.SetSize(obj.Size)
	out.SetUrls(InternalURLFromDrsAccessURLs(obj.AccessMethods))
	out.SetAuthz(InternalAuthzFromDrsAccessMethods(obj.AccessMethods))
	out.SetHashes(convertDrsChecksumsToMap(obj.Checksums))
	return out, nil
}

func syfonInternalRecordToDRSObject(rec syclient.InternalRecord) (*DRSObject, error) {
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
	if t, ok := parseRFC3339(rec.GetCreatedDate()); ok {
		obj.CreatedTime = t
	}
	if t, ok := parseRFC3339(rec.GetUpdatedDate()); ok {
		obj.UpdatedTime = t
	}
	return obj, nil
}

func convertDrsChecksumsToMap(checksums []syclient.Checksum) map[string]string {
	result := make(map[string]string, len(checksums))
	for _, c := range checksums {
		result[c.Type] = c.Checksum
	}
	return result
}

func convertMapToDrsChecksums(hashes map[string]string) []syclient.Checksum {
	result := make([]syclient.Checksum, 0, len(hashes))
	for t, c := range hashes {
		result = append(result, syclient.Checksum{
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
