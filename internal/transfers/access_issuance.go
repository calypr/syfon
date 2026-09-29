package transfers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/usage"
)

type AccessLookupResult struct {
	Found  bool
	URL    string
	Target storage.Target
	Object *drs.DrsObject
}

type AccessLookupRequest struct {
	ObjectID string
	AccessID string
}

type ResolvedAccess struct {
	ObjectID string
	AccessID string
	URL      string
}

type BulkAccessLookupResult struct {
	Requested int
	Resolved  []ResolvedAccess
	Failures  []AccessFailure
}

type AccessFailure struct {
	ObjectID string
	Err      error
}

func (s *Service) IssueAccess(ctx context.Context, request AccessLookupRequest) (AccessLookupResult, error) {
	if s == nil || s.objects == nil || s.storage == nil {
		return AccessLookupResult{}, fmt.Errorf("transfer service is not configured")
	}
	obj, err := s.objects.GetObject(ctx, strings.TrimSpace(request.ObjectID), "read")
	if err != nil {
		return AccessLookupResult{}, err
	}
	return s.issueResolvedAccess(ctx, request, obj)
}

func (s *Service) issueResolvedAccess(ctx context.Context, request AccessLookupRequest, obj *drs.DrsObject) (AccessLookupResult, error) {
	resolved, event, err := s.prepareResolvedAccess(ctx, request, obj)
	if err != nil || !resolved.Found {
		return resolved, err
	}
	if err := s.events.RecordTransferAttributionEvents(ctx, []usage.Event{event}); err != nil {
		return AccessLookupResult{}, err
	}
	return resolved, nil
}

func (s *Service) prepareResolvedAccess(ctx context.Context, request AccessLookupRequest, obj *drs.DrsObject) (AccessLookupResult, usage.Event, error) {
	sourceURL := accessURLForID(obj, request.AccessID)
	if sourceURL == "" {
		return AccessLookupResult{}, usage.Event{}, nil
	}
	target, err := s.resolveDownloadTarget(ctx, obj, sourceURL)
	if err != nil {
		return AccessLookupResult{}, usage.Event{}, err
	}
	filename := ""
	if obj.Name != nil {
		filename = objects.CleanToBasename(strings.TrimSpace(*obj.Name))
	}
	signed, err := s.sign(ctx, storage.SignRequest{Target: target, Method: http.MethodGet, ExpiresIn: s.signingExpiry, DownloadFilename: filename})
	if err != nil {
		return AccessLookupResult{}, usage.Event{}, err
	}
	event, err := s.newAccessIssuedEvent(ctx, AccessRequest{Object: obj, Target: target, AccessID: request.AccessID, Direction: usage.ProviderTransferDirectionDownload, StorageURL: sourceURL})
	if err != nil {
		return AccessLookupResult{}, usage.Event{}, err
	}
	return AccessLookupResult{Found: true, URL: signed.Location, Target: target, Object: obj}, event, nil
}

func (s *Service) IssueAccessBulk(ctx context.Context, requests []AccessLookupRequest) BulkAccessLookupResult {
	ctx = storage.WithCredentialCache(ctx)
	batch := s
	var lookups map[string]objects.LookupResult
	var lookupErr error
	if s != nil && s.scopes != nil {
		copy := *s
		copy.scopes = &batchScopeReader{source: s.scopes, entries: make(map[scopeKey]scopeLookup)}
		batch = &copy
	}
	if s == nil || s.objects == nil || s.storage == nil {
		lookupErr = fmt.Errorf("transfer service is not configured")
	} else {
		identifiers := make([]string, 0, len(requests))
		for _, request := range requests {
			identifiers = append(identifiers, strings.TrimSpace(request.ObjectID))
		}
		lookups, lookupErr = s.objects.GetObjects(ctx, identifiers, "read")
	}
	type accessOutcome struct {
		resolved AccessLookupResult
		err      error
	}
	outcomes := make([]accessOutcome, len(requests))
	events := make([]usage.Event, 0, len(requests))
	pending := make([]int, 0, len(requests))
	for i, request := range requests {
		lookup := lookups[strings.TrimSpace(request.ObjectID)]
		err := lookupErr
		if err == nil {
			err = lookup.Err
		}
		var resolved AccessLookupResult
		if err == nil && lookup.Object == nil {
			err = errorapi.ErrObjectNotFound
		}
		if err == nil {
			var event usage.Event
			resolved, event, err = batch.prepareResolvedAccess(ctx, request, lookup.Object)
			if err == nil && resolved.Found {
				pending = append(pending, i)
				events = append(events, event)
			}
		}
		if err == nil && !resolved.Found {
			err = errorapi.ErrObjectLocationUnavailable
		}
		outcomes[i] = accessOutcome{resolved: resolved, err: err}
	}
	if len(events) > 0 {
		if err := ctx.Err(); err != nil {
			for _, i := range pending {
				outcomes[i].err = err
			}
		} else if err := batch.events.RecordTransferAttributionEvents(ctx, events); err != nil {
			for eventIndex, i := range pending {
				if ctxErr := ctx.Err(); ctxErr != nil {
					outcomes[i].err = ctxErr
					continue
				}
				outcomes[i].err = batch.events.RecordTransferAttributionEvents(ctx, []usage.Event{events[eventIndex]})
			}
		}
	}
	result := BulkAccessLookupResult{Requested: len(requests), Resolved: make([]ResolvedAccess, 0, len(requests))}
	for i, request := range requests {
		if err := outcomes[i].err; err != nil {
			result.Failures = append(result.Failures, AccessFailure{ObjectID: strings.TrimSpace(request.ObjectID), Err: err})
			continue
		}
		result.Resolved = append(result.Resolved, ResolvedAccess{ObjectID: strings.TrimSpace(request.ObjectID), AccessID: strings.TrimSpace(request.AccessID), URL: outcomes[i].resolved.URL})
	}
	return result
}

type scopeKey struct {
	organization string
	project      string
}

type scopeLookup struct {
	scope buckets.Scope
	found bool
	err   error
}

type batchScopeReader struct {
	source  ScopeReader
	entries map[scopeKey]scopeLookup
}

func (r *batchScopeReader) LookupBucketScope(ctx context.Context, organization, project string) (buckets.Scope, bool, error) {
	key := scopeKey{organization: strings.TrimSpace(organization), project: strings.TrimSpace(project)}
	if cached, ok := r.entries[key]; ok {
		if err := ctx.Err(); err != nil {
			return buckets.Scope{}, false, err
		}
		return cached.scope, cached.found, cached.err
	}
	scope, found, err := r.source.LookupBucketScope(ctx, key.organization, key.project)
	r.entries[key] = scopeLookup{scope: scope, found: found, err: err}
	return scope, found, err
}

func accessURLForID(obj *drs.DrsObject, accessID string) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	accessID = strings.TrimSpace(accessID)
	if accessID == "" {
		return ""
	}
	legacyMatches := make([]string, 0, 1)
	for _, method := range *obj.AccessMethods {
		if method.AccessUrl == nil || strings.TrimSpace(method.AccessUrl.Url) == "" {
			continue
		}
		methodAccessID := ""
		if method.AccessId != nil {
			methodAccessID = *method.AccessId
		}
		if strings.EqualFold(methodAccessID, accessID) {
			return method.AccessUrl.Url
		}
		if strings.EqualFold(strings.TrimSpace(string(method.Type)), accessID) {
			legacyMatches = append(legacyMatches, method.AccessUrl.Url)
		}
	}
	if len(legacyMatches) == 1 {
		return legacyMatches[0]
	}
	return ""
}
