package testutils

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/calypr/syfon/internal/usage"
)

// MockDatabase implements db.DatabaseInterface for testing
type MockDatabase struct {
	Objects                map[string]*objects.Record
	ObjectAuthz            map[string]map[string][]string
	Credentials            map[string]buckets.Credential
	BucketScopes           map[string]buckets.Scope
	PendingMeta            map[string]transfers.PendingMetadata
	Usage                  map[string]usage.FileUsage
	TransferEvents         []usage.Event
	ProviderTransferEvents []usage.ProviderEvent
	NoDefaultCreds         bool
	GetObjectErr           error
	GetBucketScopeCalls    int
}

func (m *MockDatabase) GetObject(ctx context.Context, id string) (*objects.Record, error) {
	if m.GetObjectErr != nil {
		return nil, m.GetObjectErr
	}
	if obj, ok := m.Objects[id]; ok {
		wrapped := *obj
		if authz, ok := m.ObjectAuthz[id]; ok {
			wrapped.Authorizations = cloneAuthzMap(authz)
		}
		attachAuthorizationsToAccessMethods(&wrapped)
		return &wrapped, nil
	}
	return nil, fmt.Errorf("%w: object not found", faults.ErrNotFound)
}

func (m *MockDatabase) DeleteObject(ctx context.Context, id string) error {
	if m.Objects != nil {
		delete(m.Objects, id)
	}
	return nil
}

func (m *MockDatabase) DeleteObjectAlias(ctx context.Context, aliasID string) error {
	if m.Objects != nil {
		delete(m.Objects, aliasID)
	}
	return nil
}

func (m *MockDatabase) CreateObject(ctx context.Context, obj *objects.Record) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*objects.Record)
	}
	copyObj := *obj
	m.Objects[string(obj.Id)] = &copyObj
	if len(obj.Authorizations) > 0 {
		if m.ObjectAuthz == nil {
			m.ObjectAuthz = make(map[string]map[string][]string)
		}
		m.ObjectAuthz[string(obj.Id)] = cloneAuthzMap(obj.Authorizations)
	}
	return nil
}

func (m *MockDatabase) GetObjectsByChecksum(ctx context.Context, checksum string) ([]objects.Record, error) {
	if m.Objects == nil {
		return []objects.Record{}, nil
	}
	out := make([]objects.Record, 0, 1)
	for id, obj := range m.Objects {
		if id == checksum || string(obj.Id) == checksum {
			wrapped := *obj
			if authz, ok := m.ObjectAuthz[id]; ok {
				wrapped.Authorizations = cloneAuthzMap(authz)
			}
			attachAuthorizationsToAccessMethods(&wrapped)
			out = append(out, wrapped)
			continue
		}
		for _, c := range obj.Checksums {
			if strings.EqualFold(strings.TrimSpace(c.Checksum), strings.TrimSpace(checksum)) {
				wrapped := *obj
				if authz, ok := m.ObjectAuthz[id]; ok {
					wrapped.Authorizations = cloneAuthzMap(authz)
				}
				attachAuthorizationsToAccessMethods(&wrapped)
				out = append(out, wrapped)
				break
			}
		}
	}
	return out, nil
}

func (m *MockDatabase) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]objects.Record, error) {
	out := make(map[string][]objects.Record, len(checksums))
	for _, cs := range checksums {
		matches, err := m.GetObjectsByChecksum(ctx, cs)
		if err != nil {
			return nil, err
		}
		out[cs] = matches
	}
	return out, nil
}

func (m *MockDatabase) ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error) {
	out := make(map[string][]string, len(checksums))
	for _, checksum := range checksums {
		checksum = strings.TrimSpace(checksum)
		if checksum == "" {
			continue
		}
		out[checksum] = []string{}
		matches, err := m.GetObjectsByChecksum(ctx, checksum)
		if err != nil {
			return nil, err
		}
		seen := map[string]struct{}{}
		for _, match := range matches {
			if !objectMatchesMockScope(m.ObjectAuthz[string(match.Id)], organization, project) {
				continue
			}
			if _, ok := seen[string(match.Id)]; ok {
				continue
			}
			seen[string(match.Id)] = struct{}{}
			out[checksum] = append(out[checksum], string(match.Id))
		}
	}
	return out, nil
}

func objectMatchesMockScope(authz map[string][]string, organization, project string) bool {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		return true
	}
	projects, ok := authz[organization]
	if !ok {
		return false
	}
	if project == "" || len(projects) == 0 {
		return true
	}
	for _, candidate := range projects {
		if candidate == project {
			return true
		}
	}
	return false
}

func (m *MockDatabase) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
	ids := make([]string, 0)
	for id := range m.Objects {
		if strings.TrimSpace(organization) == "" {
			ids = append(ids, id)
			continue
		}
		authz := map[string][]string{}
		if m.ObjectAuthz != nil {
			if v, ok := m.ObjectAuthz[id]; ok {
				authz = v
			}
		}
		projects, ok := authz[organization]
		if !ok {
			continue
		}
		if strings.TrimSpace(project) == "" || len(projects) == 0 {
			ids = append(ids, id)
			continue
		}
		for _, p := range projects {
			if p == project {
				ids = append(ids, id)
				break
			}
		}
	}
	return ids, nil
}

func (m *MockDatabase) ListObjectIDsByResources(ctx context.Context, resources []string, includeUnscoped bool) ([]string, error) {
	allowed := map[string]struct{}{}
	for _, resource := range resources {
		resource = strings.TrimSpace(resource)
		if resource != "" {
			allowed[resource] = struct{}{}
		}
	}
	ids := make([]string, 0)
	for id := range m.Objects {
		authz := map[string][]string{}
		if m.ObjectAuthz != nil {
			if v, ok := m.ObjectAuthz[id]; ok {
				authz = v
			}
		}
		resourcesForObject := []string{}
		for org, projects := range authz {
			if len(projects) == 0 {
				resourcesForObject = append(resourcesForObject, "/programs/"+org)
				continue
			}
			for _, project := range projects {
				resourcesForObject = append(resourcesForObject, "/programs/"+org+"/projects/"+project)
			}
		}
		if len(resourcesForObject) == 0 {
			if includeUnscoped {
				ids = append(ids, id)
			}
			continue
		}
		for _, resource := range resourcesForObject {
			if _, ok := allowed[resource]; ok {
				ids = append(ids, id)
				break
			}
		}
	}
	return ids, nil
}

func (m *MockDatabase) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
	if m.Objects == nil {
		return fmt.Errorf("%w: object not found", faults.ErrNotFound)
	}
	obj, ok := m.Objects[canonicalObjectID]
	if !ok {
		return fmt.Errorf("%w: object not found", faults.ErrNotFound)
	}
	copyObj := *obj
	copyObj.Id = objects.RecordID(aliasID)
	m.Objects[aliasID] = &copyObj
	if m.ObjectAuthz != nil {
		if authz, ok := m.ObjectAuthz[canonicalObjectID]; ok {
			m.ObjectAuthz[aliasID] = cloneAuthzMap(authz)
		}
	}
	return nil
}

func (m *MockDatabase) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	if m.Objects != nil {
		if _, ok := m.Objects[aliasID]; ok {
			return aliasID, nil
		}
	}
	return "", fmt.Errorf("%w: object not found", faults.ErrNotFound)
}

func (m *MockDatabase) RegisterObjects(ctx context.Context, records []objects.Record) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*objects.Record)
	}
	for _, obj := range records {
		copyObj := obj
		m.Objects[string(obj.Id)] = &copyObj
		if m.ObjectAuthz == nil {
			m.ObjectAuthz = make(map[string]map[string][]string)
		}
		m.ObjectAuthz[string(obj.Id)] = cloneAuthzMap(obj.Authorizations)
	}
	return nil
}

func (m *MockDatabase) ReplaceObjects(ctx context.Context, records []objects.Record) error {
	if err := m.RegisterObjects(ctx, records); err != nil {
		return err
	}
	for _, obj := range records {
		if obj.AccessMethods == nil {
			continue
		}
		if err := m.UpdateObjectAccessMethods(ctx, string(obj.Id), *obj.AccessMethods); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockDatabase) GetBulkObjects(ctx context.Context, ids []string) ([]objects.Record, error) {
	out := make([]objects.Record, 0, len(ids))
	for _, id := range ids {
		if obj, ok := m.Objects[id]; ok {
			wrapped := *obj
			if authz, ok := m.ObjectAuthz[id]; ok {
				wrapped.Authorizations = cloneAuthzMap(authz)
			}
			attachAuthorizationsToAccessMethods(&wrapped)
			out = append(out, wrapped)
		}
	}
	return out, nil
}

func (m *MockDatabase) BulkDeleteObjects(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if m.Objects != nil {
			delete(m.Objects, id)
		}
	}
	return nil
}

func (m *MockDatabase) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []objects.AccessMethod) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*objects.Record)
	}
	obj, ok := m.Objects[objectID]
	if !ok {
		obj = &objects.Record{Id: objects.RecordID(objectID)}
		m.Objects[objectID] = obj
	}
	obj.AccessMethods = &accessMethods
	return nil
}

func (m *MockDatabase) RemoveObjectControlledAccess(ctx context.Context, objectID, resource string) error {
	obj, ok := m.Objects[objectID]
	if !ok {
		return faults.ErrNotFound
	}
	wrapped := *obj
	if authz, ok := m.ObjectAuthz[objectID]; ok {
		wrapped.Authorizations = cloneAuthzMap(authz)
	}
	resources := sycommon.AuthzMapToControlledAccess(wrapped.Authorizations)
	if wrapped.ControlledAccess != nil {
		resources = sycommon.NormalizeAccessResources(*wrapped.ControlledAccess)
	}
	target := sycommon.NormalizeAccessResources([]string{resource})
	if len(target) == 0 {
		return faults.ErrNotFound
	}
	found := false
	filtered := make([]string, 0, len(resources))
	for _, existing := range resources {
		if existing == target[0] {
			found = true
			continue
		}
		filtered = append(filtered, existing)
	}
	if !found {
		return faults.ErrNotFound
	}
	if len(filtered) == 0 {
		delete(m.ObjectAuthz, objectID)
		obj.ControlledAccess = nil
		return nil
	}
	obj.ControlledAccess = &filtered
	if m.ObjectAuthz == nil {
		m.ObjectAuthz = make(map[string]map[string][]string)
	}
	m.ObjectAuthz[objectID] = sycommon.ControlledAccessToAuthzMap(filtered)
	return nil
}

func (m *MockDatabase) RemoveObjectControlledAccessBulk(ctx context.Context, objectIDs []string, resource string) (int, error) {
	target := sycommon.NormalizeAccessResources([]string{resource})
	if len(target) == 0 {
		return 0, faults.ErrNotFound
	}
	orgWide := !strings.Contains(target[0], "/project/")
	count := 0
	for _, objectID := range objectIDs {
		obj, ok := m.Objects[objectID]
		if !ok {
			return count, faults.ErrNotFound
		}
		wrapped := *obj
		if objectAuthz, ok := m.ObjectAuthz[objectID]; ok {
			wrapped.Authorizations = cloneAuthzMap(objectAuthz)
		}
		resources := sycommon.AuthzMapToControlledAccess(wrapped.Authorizations)
		if wrapped.ControlledAccess != nil {
			resources = sycommon.NormalizeAccessResources(*wrapped.ControlledAccess)
		}
		for _, existing := range resources {
			if existing != target[0] && (!orgWide || !strings.HasPrefix(existing, target[0]+"/project/")) {
				continue
			}
			if err := m.RemoveObjectControlledAccess(ctx, objectID, existing); err != nil {
				return count, err
			}
			count++
		}
	}
	return count, nil
}

func (m *MockDatabase) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]objects.AccessMethod) error {
	for objectID, accessMethods := range updates {
		if err := m.UpdateObjectAccessMethods(ctx, objectID, accessMethods); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockDatabase) GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error) {
	if m.Credentials != nil {
		if cred, ok := m.Credentials[bucket]; ok {
			c := cred
			return &c, nil
		}
		for _, cred := range m.Credentials {
			if strings.TrimSpace(cred.Bucket) == strings.TrimSpace(bucket) {
				c := cred
				return &c, nil
			}
		}
	}
	if m.NoDefaultCreds {
		return nil, nil
	}
	return &buckets.Credential{
		CredentialID: bucket,
		Bucket:       bucket,
		Provider:     "s3",
		Region:       "us-east-1",
		AccessKey:    "test-key",
		SecretKey:    "test-secret",
	}, nil
}

func (m *MockDatabase) SaveS3Credential(ctx context.Context, cred *buckets.Credential) error {
	if m.Credentials == nil {
		m.Credentials = make(map[string]buckets.Credential)
	}
	key := strings.TrimSpace(cred.CredentialID)
	if key == "" {
		key = strings.TrimSpace(cred.Bucket)
	}
	m.Credentials[key] = *cred
	return nil
}

func (m *MockDatabase) DeleteS3Credential(ctx context.Context, bucket string) error {
	if m.Credentials != nil {
		delete(m.Credentials, bucket)
	}
	return nil
}

func (m *MockDatabase) ListS3Credentials(ctx context.Context) ([]buckets.Credential, error) {
	if len(m.Credentials) > 0 {
		out := make([]buckets.Credential, 0, len(m.Credentials))
		for _, v := range m.Credentials {
			out = append(out, v)
		}
		return out, nil
	}
	if m.NoDefaultCreds {
		return []buckets.Credential{}, nil
	}
	return []buckets.Credential{
		{CredentialID: "test-bucket-1", Bucket: "test-bucket-1", Provider: "s3", Region: "us-east-1"},
	}, nil
}

func bucketScopeKey(org, project string) string {
	return strings.TrimSpace(org) + "|" + strings.TrimSpace(project)
}

func (m *MockDatabase) CreateBucketScope(ctx context.Context, scope *buckets.Scope) error {
	if scope == nil {
		return fmt.Errorf("scope is required")
	}
	if m.BucketScopes == nil {
		m.BucketScopes = make(map[string]buckets.Scope)
	}
	k := bucketScopeKey(scope.Organization, scope.ProjectID)
	m.BucketScopes[k] = *scope
	return nil
}

func (m *MockDatabase) DeleteBucketScope(ctx context.Context, organization, projectID, credentialID, pathPrefix string) error {
	if m.BucketScopes == nil {
		return fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	k := bucketScopeKey(organization, projectID)
	scope, ok := m.BucketScopes[k]
	if !ok {
		return fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	if strings.TrimSpace(scope.CredentialID) != strings.TrimSpace(credentialID) &&
		strings.TrimSpace(scope.Bucket) != strings.TrimSpace(credentialID) {
		return fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	if strings.Trim(strings.TrimSpace(scope.PathPrefix), "/") != strings.Trim(strings.TrimSpace(pathPrefix), "/") {
		return fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	delete(m.BucketScopes, k)
	return nil
}

func (m *MockDatabase) GetBucketScope(ctx context.Context, organization, projectID string) (*buckets.Scope, error) {
	m.GetBucketScopeCalls++
	if m.BucketScopes == nil {
		return nil, fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	k := bucketScopeKey(organization, projectID)
	s, ok := m.BucketScopes[k]
	if !ok {
		return nil, fmt.Errorf("%w: bucket scope not found", faults.ErrNotFound)
	}
	cp := s
	return &cp, nil
}

func (m *MockDatabase) ListBucketScopes(ctx context.Context) ([]buckets.Scope, error) {
	out := make([]buckets.Scope, 0, len(m.BucketScopes))
	for _, s := range m.BucketScopes {
		out = append(out, s)
	}
	return out, nil
}

func (m *MockDatabase) SavePendingLFSMeta(ctx context.Context, entries []transfers.PendingMetadata) error {
	if m.PendingMeta == nil {
		m.PendingMeta = make(map[string]transfers.PendingMetadata)
	}
	for _, e := range entries {
		m.PendingMeta[e.OID] = e
	}
	return nil
}

func (m *MockDatabase) GetPendingLFSMeta(ctx context.Context, oid string) (*transfers.PendingMetadata, error) {
	if m.PendingMeta == nil {
		return nil, fmt.Errorf("%w: pending metadata not found", faults.ErrNotFound)
	}
	e, ok := m.PendingMeta[oid]
	if !ok {
		return nil, fmt.Errorf("%w: pending metadata not found", faults.ErrNotFound)
	}
	return &e, nil
}

func (m *MockDatabase) PopPendingLFSMeta(ctx context.Context, oid string) (*transfers.PendingMetadata, error) {
	if m.PendingMeta == nil {
		return nil, fmt.Errorf("%w: pending metadata not found", faults.ErrNotFound)
	}
	e, ok := m.PendingMeta[oid]
	if !ok {
		return nil, fmt.Errorf("%w: pending metadata not found", faults.ErrNotFound)
	}
	delete(m.PendingMeta, oid)
	return &e, nil
}

func (m *MockDatabase) RecordFileUpload(ctx context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = make(map[string]usage.FileUsage)
	}
	u := m.Usage[objectID]
	u.ObjectID = objectID
	u.UploadCount++
	now := time.Now().UTC()
	u.LastUploadTime = &now
	if obj, ok := m.Objects[objectID]; ok {
		u.Name = common.StringVal(obj.Name)
		u.Size = obj.Size
	}
	if u.LastAccessTime == nil || now.After(*u.LastAccessTime) {
		t := now
		u.LastAccessTime = &t
	}
	m.Usage[objectID] = u
	return nil
}

func (m *MockDatabase) RecordFileDownload(ctx context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = make(map[string]usage.FileUsage)
	}
	u := m.Usage[objectID]
	u.ObjectID = objectID
	u.DownloadCount++
	now := time.Now().UTC()
	u.LastDownloadTime = &now
	if obj, ok := m.Objects[objectID]; ok {
		u.Name = common.StringVal(obj.Name)
		u.Size = obj.Size
	}
	if u.LastAccessTime == nil || now.After(*u.LastAccessTime) {
		t := now
		u.LastAccessTime = &t
	}
	m.Usage[objectID] = u
	return nil
}

func (m *MockDatabase) GetFileUsage(ctx context.Context, objectID string) (*usage.FileUsage, error) {
	if m.Usage == nil {
		return nil, fmt.Errorf("%w: file usage not found", faults.ErrNotFound)
	}
	u, ok := m.Usage[objectID]
	if !ok {
		return nil, fmt.Errorf("%w: file usage not found", faults.ErrNotFound)
	}
	copyUsage := u
	return &copyUsage, nil
}

func (m *MockDatabase) ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]usage.FileUsage, error) {
	out := make([]usage.FileUsage, 0, len(ids))
	for _, id := range ids {
		if m.Usage != nil {
			if fileUsage, ok := m.Usage[id]; ok {
				out = append(out, fileUsage)
				continue
			}
		}
		if obj, ok := m.Objects[id]; ok {
			out = append(out, usage.FileUsage{
				ObjectID: id,
				Name:     common.StringVal(obj.Name),
				Size:     obj.Size,
			})
		}
	}
	return out, nil
}

func (m *MockDatabase) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]usage.FileUsage, error) {
	out := make([]usage.FileUsage, 0)
	if m.Usage == nil {
		return out, nil
	}
	for _, u := range m.Usage {
		if inactiveSince != nil {
			if u.LastDownloadTime != nil && !u.LastDownloadTime.Before(*inactiveSince) {
				continue
			}
		}
		out = append(out, u)
	}
	if offset >= len(out) {
		return []usage.FileUsage{}, nil
	}
	if limit <= 0 {
		return out[offset:], nil
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}

func (m *MockDatabase) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (usage.FileUsageSummary, error) {
	var s usage.FileUsageSummary
	s.TotalFiles = int64(len(m.Objects))
	for _, u := range m.Usage {
		s.TotalUploads += u.UploadCount
		s.TotalDownloads += u.DownloadCount
		if inactiveSince == nil {
			continue
		}
		if u.LastDownloadTime == nil || u.LastDownloadTime.Before(*inactiveSince) {
			s.InactiveFileCount++
		}
	}
	return s, nil
}

func (m *MockDatabase) RecordTransferAttributionEvents(ctx context.Context, events []usage.Event) error {
	if len(events) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(m.TransferEvents))
	for _, ev := range m.TransferEvents {
		if ev.EventID != "" {
			seen[ev.EventID] = true
		}
	}
	for _, ev := range events {
		if ev.EventID != "" && seen[ev.EventID] {
			continue
		}
		if ev.EventID != "" {
			seen[ev.EventID] = true
		}
		m.TransferEvents = append(m.TransferEvents, ev)
	}
	return nil
}

func (m *MockDatabase) RecordProviderTransferEvents(ctx context.Context, events []usage.ProviderEvent) error {
	if len(events) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(m.ProviderTransferEvents))
	for _, ev := range m.ProviderTransferEvents {
		if ev.ProviderEventID != "" {
			seen[ev.ProviderEventID] = true
		}
	}
	for i := range events {
		ev := m.reconcileProviderTransferEvent(events[i])
		events[i] = ev
		if ev.ProviderEventID != "" && seen[ev.ProviderEventID] {
			continue
		}
		if ev.ProviderEventID != "" {
			seen[ev.ProviderEventID] = true
		}
		m.ProviderTransferEvents = append(m.ProviderTransferEvents, ev)
	}
	return nil
}

func (m *MockDatabase) reconcileProviderTransferEvent(ev usage.ProviderEvent) usage.ProviderEvent {
	if ev.ReconciliationStatus == "" {
		ev.ReconciliationStatus = usage.ProviderTransferUnmatched
	}
	for _, grant := range m.TransferEvents {
		if grant.EventType != usage.TransferEventAccessIssued {
			continue
		}
		if ev.AccessGrantID != "" && (ev.AccessGrantID == grant.AccessGrantID || ev.AccessGrantID == grant.EventID) {
			mockMergeAccessGrantIntoProviderEvent(&ev, grant)
			ev.ReconciliationStatus = usage.ProviderTransferMatched
			return ev
		}
		if ev.AccessGrantID == "" && ev.Provider == grant.Provider && ev.Bucket == grant.Bucket &&
			(ev.StorageURL == grant.StorageURL || ev.StorageURL == "" && strings.HasSuffix(grant.StorageURL, "/"+strings.TrimLeft(ev.ObjectKey, "/"))) {
			mockMergeAccessGrantIntoProviderEvent(&ev, grant)
			ev.ReconciliationStatus = usage.ProviderTransferMatched
			return ev
		}
	}
	return ev
}

func mockMergeAccessGrantIntoProviderEvent(ev *usage.ProviderEvent, grant usage.Event) {
	if ev.AccessGrantID == "" {
		ev.AccessGrantID = grant.AccessGrantID
		if ev.AccessGrantID == "" {
			ev.AccessGrantID = grant.EventID
		}
	}
	if ev.ObjectID == "" {
		ev.ObjectID = grant.ObjectID
	}
	if ev.SHA256 == "" {
		ev.SHA256 = grant.SHA256
	}
	if ev.ObjectSize == 0 {
		ev.ObjectSize = grant.ObjectSize
	}
	if ev.Organization == "" {
		ev.Organization = grant.Organization
	}
	if ev.Project == "" {
		ev.Project = grant.Project
	}
	if ev.AccessID == "" {
		ev.AccessID = grant.AccessID
	}
	if ev.StorageURL == "" {
		ev.StorageURL = grant.StorageURL
	}
	hasActor := ev.ActorEmail != "" || ev.ActorSubject != ""
	if !hasActor {
		ev.ActorEmail = grant.ActorEmail
	}
	if !hasActor {
		ev.ActorSubject = grant.ActorSubject
	}
	if ev.AuthMode == "" {
		ev.AuthMode = grant.AuthMode
	}
}

func (m *MockDatabase) GetTransferAttributionSummary(ctx context.Context, filter usage.Filter) (usage.Summary, error) {
	var summary usage.Summary
	for _, ev := range m.TransferEvents {
		if !transferEventMatchesFilter(ev, filter) {
			continue
		}
		summary.EventCount++
		if ev.EventType == usage.TransferEventAccessIssued {
			summary.AccessIssuedCount++
		}
		summary.BytesRequested += ev.BytesRequested
		switch ev.Direction {
		case usage.ProviderTransferDirectionDownload:
			summary.DownloadEventCount++
			summary.BytesDownloaded += ev.BytesRequested
		case usage.ProviderTransferDirectionUpload:
			summary.UploadEventCount++
			summary.BytesUploaded += ev.BytesRequested
		}
	}
	return summary, nil
}

func (m *MockDatabase) GetTransferAttributionBreakdown(ctx context.Context, filter usage.Filter, groupBy string) ([]usage.Breakdown, error) {
	items := map[string]*usage.Breakdown{}
	for _, ev := range m.TransferEvents {
		if !transferEventMatchesFilter(ev, filter) {
			continue
		}
		key := transferBreakdownKey(ev, groupBy)
		item := items[key]
		if item == nil {
			item = &usage.Breakdown{
				Key:          key,
				Organization: ev.Organization,
				Project:      ev.Project,
				Provider:     ev.Provider,
				Bucket:       ev.Bucket,
				SHA256:       ev.SHA256,
				ActorEmail:   ev.ActorEmail,
				ActorSubject: ev.ActorSubject,
			}
			items[key] = item
		}
		item.EventCount++
		item.BytesRequested += ev.BytesRequested
		if ev.Direction == usage.ProviderTransferDirectionDownload {
			item.BytesDownloaded += ev.BytesRequested
		}
		if ev.Direction == usage.ProviderTransferDirectionUpload {
			item.BytesUploaded += ev.BytesRequested
		}
		t := ev.EventTime
		if item.LastTransferTime == nil || (!t.IsZero() && t.After(*item.LastTransferTime)) {
			item.LastTransferTime = &t
		}
	}
	out := make([]usage.Breakdown, 0, len(items))
	for _, item := range items {
		out = append(out, *item)
	}
	return out, nil
}

func transferEventMatchesFilter(ev usage.Event, filter usage.Filter) bool {
	if filter.Organization != "" && ev.Organization != filter.Organization {
		return false
	}
	if filter.Project != "" && ev.Project != filter.Project {
		return false
	}
	if filter.EventType != "" && filter.EventType != "all" && ev.EventType != filter.EventType {
		return false
	}
	direction := filter.Direction
	if direction == "" {
		switch filter.EventType {
		case usage.ProviderTransferDirectionDownload:
			direction = usage.ProviderTransferDirectionDownload
		case usage.ProviderTransferDirectionUpload:
			direction = usage.ProviderTransferDirectionUpload
		}
	}
	if direction != "" && direction != "all" && ev.Direction != direction {
		return false
	}
	if filter.From != nil && ev.EventTime.Before(*filter.From) {
		return false
	}
	if filter.To != nil && ev.EventTime.After(*filter.To) {
		return false
	}
	if filter.Provider != "" && ev.Provider != filter.Provider {
		return false
	}
	if filter.Bucket != "" && ev.Bucket != filter.Bucket {
		return false
	}
	if filter.SHA256 != "" && ev.SHA256 != filter.SHA256 {
		return false
	}
	if filter.User != "" && ev.ActorEmail != filter.User && ev.ActorSubject != filter.User {
		return false
	}
	return true
}

func transferBreakdownKey(ev usage.Event, groupBy string) string {
	switch groupBy {
	case "user":
		if ev.ActorEmail != "" {
			return ev.ActorEmail
		}
		return ev.ActorSubject
	case "provider":
		return ev.Provider + ":" + ev.Bucket
	case "object":
		return ev.SHA256
	default:
		return ev.Organization + "/" + ev.Project
	}
}

func cloneAuthzMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for org, projects := range in {
		out[org] = append([]string(nil), projects...)
	}
	return out
}

func attachAuthorizationsToAccessMethods(obj *objects.Record) {
}

// MockUrlManager implements urlmanager.UrlManager for testing
type MockUrlManager struct{}

func (m *MockUrlManager) SignURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	suffix := "?signed=true"
	if opts.Method == http.MethodPut || opts.Method == http.MethodPost {
		suffix += "&upload=true"
	}
	return url + suffix, nil
}

func (m *MockUrlManager) SignUploadURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	return url + "?signed=true&upload=true", nil
}

func (m *MockUrlManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	return "mock-upload-id", nil
}

func (m *MockUrlManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	return fmt.Sprintf("s3://%s/%s?uploadId=%s&partNumber=%d", bucket, key, uploadId, partNumber), nil
}

func (m *MockUrlManager) SignDownloadPart(ctx context.Context, accessId string, url string, start int64, end int64, opts urlmanager.SignOptions) (string, error) {
	return fmt.Sprintf("%s?signed=true&range=%d-%d", url, start, end), nil
}

func (m *MockUrlManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []urlmanager.MultipartPart) error {
	return nil
}
