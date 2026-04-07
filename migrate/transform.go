package migrate

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

// indexdDateFormats lists the date formats that Indexd and Syfon use,
// in preference order.
var indexdDateFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000000", // Indexd microseconds without timezone
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// TransformError captures a failed transformation together with the source DID.
type TransformError struct {
	DID string
	Err error
}

func (e TransformError) Error() string {
	return fmt.Sprintf("transform %s: %v", e.DID, e.Err)
}

// Transform converts a single IndexdRecord into a core.InternalObject using
// the field mapping defined in GitHub issue #20.
//
// The DID is preserved as-is (including prefixes such as "dg.FOO/").
// Deprecated fields (baseid, rev, uploader, acl, form, metadata,
// urls_metadata) are silently dropped.
func Transform(rec IndexdRecord) (core.InternalObject, error) {
	did := strings.TrimSpace(rec.DID)
	if did == "" {
		return core.InternalObject{}, fmt.Errorf("record has empty did")
	}

	obj := drs.DrsObject{
		Id:          did,
		SelfUri:     "drs://" + did,
		Size:        rec.Size,
		Name:        rec.FileName,
		Version:     rec.Version,
		Description: rec.Description,
		CreatedTime: parseIndexdDate(rec.CreatedDate),
		UpdatedTime: parseIndexdDate(rec.UpdatedDate),
	}

	// Zero-value timestamps fall back to now so the record is always valid.
	if obj.CreatedTime.IsZero() {
		obj.CreatedTime = time.Now().UTC()
	}
	if obj.UpdatedTime.IsZero() {
		obj.UpdatedTime = obj.CreatedTime
	}

	// urls[] → drs_object_access_method
	for _, rawURL := range rec.URLs {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			AccessUrl: drs.AccessMethodAccessUrl{Url: rawURL},
			Type:      inferAccessMethodType(rawURL),
		})
	}

	// hashes → drs_object_checksum
	for typ, checksum := range rec.Hashes {
		typ = strings.TrimSpace(typ)
		checksum = strings.TrimSpace(checksum)
		if typ == "" || checksum == "" {
			continue
		}
		obj.Checksums = append(obj.Checksums, drs.Checksum{
			Type:     typ,
			Checksum: checksum,
		})
	}

	// authz[] → drs_object_authz
	authz := make([]string, 0, len(rec.Authz))
	seen := make(map[string]struct{}, len(rec.Authz))
	for _, res := range rec.Authz {
		res = strings.TrimSpace(res)
		if res == "" {
			continue
		}
		if _, ok := seen[res]; ok {
			continue
		}
		seen[res] = struct{}{}
		authz = append(authz, res)
	}

	// Propagate authz to each access method's bearer issuers field.
	if len(authz) > 0 {
		for i := range obj.AccessMethods {
			obj.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: append([]string(nil), authz...),
			}
		}
	}

	return core.InternalObject{
		DrsObject:      obj,
		Authorizations: authz,
	}, nil
}

// TransformBatch converts a slice of IndexdRecords, collecting per-record
// errors without aborting early.  Successfully transformed objects are
// returned alongside any errors encountered.
func TransformBatch(records []IndexdRecord) ([]core.InternalObject, []TransformError) {
	objects := make([]core.InternalObject, 0, len(records))
	var errs []TransformError
	for _, rec := range records {
		obj, err := Transform(rec)
		if err != nil {
			errs = append(errs, TransformError{DID: rec.DID, Err: err})
			continue
		}
		objects = append(objects, obj)
	}
	return objects, errs
}

// parseIndexdDate parses a date string using the known Indexd/Syfon formats.
// Returns the zero value when parsing fails so callers can substitute a
// sensible default.
func parseIndexdDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range indexdDateFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// inferAccessMethodType returns the DRS access method type derived from a URL
// scheme.  Falls back to "https" for unknown or empty schemes.
func inferAccessMethodType(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "https"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" {
		return "https"
	}
	switch strings.ToLower(parsed.Scheme) {
	case "s3":
		return "s3"
	case "gs":
		return "gs"
	case "az", "azure":
		return "az"
	case "file":
		return "file"
	case "ftp":
		return "ftp"
	case "http":
		return "http"
	default:
		return "https"
	}
}

