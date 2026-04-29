package migrate

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/models"
)

var indexdDateFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05.999999Z",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

type TransformError struct {
	DID string
	Err error
}

func (e TransformError) Error() string {
	return fmt.Sprintf("transform %s: %v", e.DID, e.Err)
}

func Transform(rec IndexdRecord, defaultAuthz []string, now time.Time) (MigrationRecord, error) {
	did := strings.TrimSpace(rec.DID)
	if did == "" {
		return MigrationRecord{}, fmt.Errorf("record has empty did")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	authz := uniqueStrings(rec.Authz)
	if len(authz) == 0 {
		authz = uniqueStrings(defaultAuthz)
	}
	authzMap := syfoncommon.AuthzListToMap(authz)

	checksums := checksumsFromHashes(rec.Hashes)
	created := firstParsedTime(now, rec.CreatedTime, rec.CreatedDate, rec.ContentCreatedDate)
	updated := firstParsedTime(created, rec.UpdatedTime, rec.UpdatedDate, rec.ContentUpdatedDate)
	accessMethods := accessMethodsFromURLs(rec.URLs, authzMap)

	return MigrationRecord{
		ID:            did,
		Name:          stringPtrValue(rec.FileName),
		Size:          int64PtrValue(rec.Size),
		Version:       stringPtrValue(rec.Version),
		Description:   stringPtrValue(rec.Description),
		CreatedTime:   created,
		UpdatedTime:   &updated,
		Checksums:     checksums,
		AccessMethods: accessMethods,
		Authz:         authz,
	}, nil
}

func TransformBatch(records []IndexdRecord, defaultAuthz []string, now time.Time) ([]MigrationRecord, []TransformError) {
	out := make([]MigrationRecord, 0, len(records))
	var errs []TransformError
	for _, rec := range records {
		mr, err := Transform(rec, defaultAuthz, now)
		if err != nil {
			errs = append(errs, TransformError{DID: rec.DID, Err: err})
			continue
		}
		out = append(out, mr)
	}
	return out, errs
}

func Validate(record MigrationRecord) error {
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("id is empty")
	}
	if len(record.Checksums) == 0 {
		return fmt.Errorf("no checksums")
	}
	for _, checksum := range record.Checksums {
		if strings.TrimSpace(checksum.Type) == "" || strings.TrimSpace(checksum.Checksum) == "" {
			return fmt.Errorf("checksum entry has empty type or value")
		}
	}
	return nil
}

func MigrationRecordToInternalObject(record MigrationRecord) (models.InternalObject, error) {
	if err := Validate(record); err != nil {
		return models.InternalObject{}, err
	}
	authzMap := syfoncommon.AuthzListToMap(record.Authz)
	updated := record.UpdatedTime
	if updated == nil {
		t := record.CreatedTime
		updated = &t
	}
	obj := drs.DrsObject{
		Id:            record.ID,
		SelfUri:       "drs://" + record.ID,
		Size:          record.Size,
		Name:          record.Name,
		Version:       record.Version,
		Description:   record.Description,
		CreatedTime:   record.CreatedTime,
		UpdatedTime:   updated,
		Checksums:     append([]drs.Checksum(nil), record.Checksums...),
		AccessMethods: nil,
	}
	if len(record.AccessMethods) > 0 {
		methods := append([]drs.AccessMethod(nil), record.AccessMethods...)
		for i := range methods {
			if methods[i].Authorizations == nil && authzMap != nil {
				methods[i].Authorizations = &authzMap
			}
		}
		obj.AccessMethods = &methods
	}
	return models.InternalObject{
		DrsObject:      obj,
		Authorizations: authzMap,
	}, nil
}

func firstParsedTime(fallback time.Time, values ...*string) time.Time {
	for _, value := range values {
		if value == nil {
			continue
		}
		if parsed := parseIndexdDate(*value); !parsed.IsZero() {
			return parsed
		}
	}
	return fallback.UTC()
}

func parseIndexdDate(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range indexdDateFormats {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func checksumsFromHashes(hashes map[string]string) []drs.Checksum {
	if len(hashes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(hashes))
	for key := range hashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]drs.Checksum, 0, len(keys))
	for _, typ := range keys {
		checksum := strings.TrimSpace(hashes[typ])
		typ = strings.TrimSpace(typ)
		if typ == "" || checksum == "" {
			continue
		}
		out = append(out, drs.Checksum{Type: typ, Checksum: checksum})
	}
	return out
}

func accessMethodsFromURLs(urls []string, authzMap map[string][]string) []drs.AccessMethod {
	if len(urls) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]drs.AccessMethod, 0, len(urls))
	for _, rawURL := range urls {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		if _, ok := seen[rawURL]; ok {
			continue
		}
		seen[rawURL] = struct{}{}
		methodType := accessMethodType(rawURL)
		accessID := string(methodType)
		method := drs.AccessMethod{
			Type:     methodType,
			AccessId: &accessID,
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: rawURL},
		}
		if authzMap != nil {
			method.Authorizations = &authzMap
		}
		out = append(out, method)
	}
	return out
}

func accessMethodType(rawURL string) drs.AccessMethodType {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" {
		return drs.AccessMethodTypeHttps
	}
	return drs.AccessMethodType(strings.ToLower(parsed.Scheme))
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func stringPtrValue(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func int64PtrValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
