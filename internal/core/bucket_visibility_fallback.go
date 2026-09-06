package core

import (
	"context"
	"errors"
	"strings"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
)

var (
	errBucketVisibilityScopeQuery   = errors.New("bucket visibility fallback requires an object scope query")
	errBucketVisibilityRecordReader = errors.New("bucket visibility fallback requires an object record reader")
)

// NewBucketVisibilityFallback builds the object-derived bucket visibility
// projection from the two object capabilities it needs. The returned callback
// owns no ObjectManager and returns only bucket-domain rows; object records and
// object-specific authorization stay on this composition side of the seam.
func NewBucketVisibilityFallback(scope objects.ScopeQuery, reader objects.RecordReader) buckets.VisibilityFallback {
	fallback := bucketVisibilityFallback{
		scope:  scope,
		reader: reader,
	}
	return fallback.rows
}

type bucketVisibilityFallback struct {
	scope  objects.ScopeQuery
	reader objects.RecordReader
}

func (f bucketVisibilityFallback) rows(ctx context.Context) ([]buckets.VisibilityRow, error) {
	if f.scope == nil {
		return nil, errBucketVisibilityScopeQuery
	}
	if f.reader == nil {
		return nil, errBucketVisibilityRecordReader
	}

	ids, err := f.scope.ListObjectIDsByScope(ctx, "", "")
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []buckets.VisibilityRow{}, nil
	}

	records, err := f.reader.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}

	rows := make([]buckets.VisibilityRow, 0)
	for i := range records {
		obj := &records[i]
		if !bucketVisibilityObjectReadable(ctx, obj) {
			continue
		}

		resources := ObjectAccessResources(obj)
		if len(resources) == 0 || obj.AccessMethods == nil {
			continue
		}
		for _, method := range *obj.AccessMethods {
			if method.AccessUrl == nil {
				continue
			}
			accessURL := strings.TrimSpace(method.AccessUrl.Url)
			if accessURL == "" {
				continue
			}
			for _, resource := range resources {
				resource = strings.TrimSpace(resource)
				if resource == "" {
					continue
				}
				rows = append(rows, buckets.VisibilityRow{
					AccessURL:  accessURL,
					AccessType: strings.TrimSpace(method.Type),
					Resource:   resource,
				})
			}
		}
	}
	return rows, nil
}

// bucketVisibilityObjectReadable preserves the legacy fallback's distinction
// between a broad caller (which may inspect all hydrated objects) and a
// resource-restricted caller (which must pass the object's read policy).
func bucketVisibilityObjectReadable(ctx context.Context, obj *objects.Record) bool {
	if !access.IsAuthzEnforced(ctx) ||
		access.HasMethodAccess(ctx, objectMethodRead, []string{"/programs"}) ||
		access.HasMethodAccess(ctx, objectMethodRead, []string{"/data_file"}) {
		return true
	}
	if obj != nil && obj.PublicRead {
		return true
	}
	resources := ObjectAccessResources(obj)
	if obj != nil && obj.PublicReadPolicyKnown && len(resources) == 0 {
		return false
	}
	return access.HasObjectMethodAccess(ctx, objectMethodRead, resources)
}
