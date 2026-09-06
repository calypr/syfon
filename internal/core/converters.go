package core

import (
	"time"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage/address"
)

// FirstSupportedAccessURL returns the first URL from an object that Syfon can sign.
func FirstSupportedAccessURL(obj *objects.Record) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		scheme := address.SchemeFromURL(am.AccessUrl.Url)
		if scheme != "" && address.ProviderFromScheme(scheme) == "" {
			continue
		}
		return am.AccessUrl.Url
	}
	return ""
}

// MergeRecordUpdate merges an update into an existing object.
func MergeRecordUpdate(existing objects.Record, update objects.Record, id string, now time.Time) (objects.Record, error) {
	return objects.MergeRecordUpdate(existing, update, id, now)
}
