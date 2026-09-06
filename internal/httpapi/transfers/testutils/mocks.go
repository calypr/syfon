package testutils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

// ObjectPorts exposes the object capabilities used by the transfer HTTP
// fixture. It keeps the fixture composition explicit without importing the
// production core façade.
type ObjectPorts struct {
	Reader        objects.RecordReader
	Writer        objects.RecordWriter
	AccessMethods objects.AccessMethodWriter
	AccessPolicy  objects.AccessPolicyWriter
	Aliases       objects.AliasStore
	Content       objects.ContentReader
	ChecksumScope objects.ChecksumScopeQuery
	Scope         objects.ScopeQuery
	Resources     objects.OptionalResourceQuery
	Pages         objects.OptionalPageQuery
	URLPages      objects.OptionalURLQuery
	Authorized    objects.OptionalAuthorizedQuery
}

func ObjectPortsFor(store any) ObjectPorts {
	db := store.(*MockDatabase)
	return ObjectPorts{
		Reader:        db,
		Writer:        db,
		AccessMethods: db,
		AccessPolicy:  db,
		Aliases:       db,
		Content:       db,
		ChecksumScope: db,
		Scope:         db,
	}
}

type MockDatabase struct {
	Objects             map[string]*objects.Record
	ObjectAuthz         map[string]map[string][]string
	Credentials         map[string]buckets.Credential
	BucketScopes        map[string]buckets.Scope
	PendingMeta         map[string]transfers.PendingMetadata
	Usage               map[string]usage.FileUsage
	TransferEvents      []usage.Event
	NoDefaultCreds      bool
	GetObjectErr        error
	GetBucketScopeCalls int
}

func NewInMemoryDB() *MockDatabase {
	return &MockDatabase{
		Objects:      map[string]*objects.Record{},
		Credentials:  map[string]buckets.Credential{},
		BucketScopes: map[string]buckets.Scope{},
	}
}

func (m *MockDatabase) GetObject(_ context.Context, id string) (*objects.Record, error) {
	if m.GetObjectErr != nil {
		return nil, m.GetObjectErr
	}
	obj, ok := m.Objects[id]
	if !ok {
		return nil, fmt.Errorf("%w: object not found", faults.ErrNotFound)
	}
	return m.copyObject(id, obj), nil
}

func (m *MockDatabase) GetBulkObjects(_ context.Context, ids []string) ([]objects.Record, error) {
	out := make([]objects.Record, 0, len(ids))
	for _, id := range ids {
		if obj, ok := m.Objects[id]; ok {
			out = append(out, *m.copyObject(id, obj))
		}
	}
	return out, nil
}

func (m *MockDatabase) DeleteObject(_ context.Context, id string) error {
	delete(m.Objects, id)
	return nil
}

func (m *MockDatabase) CreateObject(_ context.Context, obj *objects.Record) error {
	if m.Objects == nil {
		m.Objects = map[string]*objects.Record{}
	}
	copyObj := *obj
	m.Objects[string(obj.Id)] = &copyObj
	return nil
}

func (m *MockDatabase) BulkDeleteObjects(_ context.Context, ids []string) error {
	for _, id := range ids {
		delete(m.Objects, id)
	}
	return nil
}

func (m *MockDatabase) RegisterObjects(_ context.Context, records []objects.Record) error {
	if m.Objects == nil {
		m.Objects = map[string]*objects.Record{}
	}
	if m.ObjectAuthz == nil {
		m.ObjectAuthz = map[string]map[string][]string{}
	}
	for _, obj := range records {
		copyObj := obj
		m.Objects[string(obj.Id)] = &copyObj
		m.ObjectAuthz[string(obj.Id)] = cloneAuthzMap(obj.Authorizations)
	}
	return nil
}

func (m *MockDatabase) ReplaceObjects(ctx context.Context, records []objects.Record) error {
	return m.RegisterObjects(ctx, records)
}

func (m *MockDatabase) UpdateObjectAccessMethods(_ context.Context, objectID string, accessMethods []objects.AccessMethod) error {
	if m.Objects == nil {
		m.Objects = map[string]*objects.Record{}
	}
	obj := m.Objects[objectID]
	if obj == nil {
		obj = &objects.Record{Id: objects.RecordID(objectID)}
		m.Objects[objectID] = obj
	}
	obj.AccessMethods = &accessMethods
	return nil
}

func (m *MockDatabase) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]objects.AccessMethod) error {
	for id, methods := range updates {
		if err := m.UpdateObjectAccessMethods(ctx, id, methods); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockDatabase) RemoveObjectControlledAccess(_ context.Context, objectID, resource string) error {
	obj, ok := m.Objects[objectID]
	if !ok {
		return faults.ErrNotFound
	}
	if obj.ControlledAccess == nil {
		return faults.ErrNotFound
	}
	filtered := make([]string, 0, len(*obj.ControlledAccess))
	found := false
	for _, candidate := range *obj.ControlledAccess {
		if candidate == resource {
			found = true
			continue
		}
		filtered = append(filtered, candidate)
	}
	if !found {
		return faults.ErrNotFound
	}
	if len(filtered) == 0 {
		obj.ControlledAccess = nil
	} else {
		obj.ControlledAccess = &filtered
	}
	return nil
}

func (m *MockDatabase) RemoveObjectControlledAccessBulk(ctx context.Context, ids []string, resource string) (int, error) {
	count := 0
	for _, id := range ids {
		if err := m.RemoveObjectControlledAccess(ctx, id, resource); err != nil {
			if err == faults.ErrNotFound {
				continue
			}
			return count, err
		}
		count++
	}
	return count, nil
}

func (m *MockDatabase) DeleteObjectAlias(ctx context.Context, aliasID string) error {
	return m.DeleteObject(ctx, aliasID)
}

func (m *MockDatabase) CreateObjectAlias(_ context.Context, aliasID, canonicalObjectID string) error {
	obj, ok := m.Objects[canonicalObjectID]
	if !ok {
		return faults.ErrNotFound
	}
	copyObj := *obj
	copyObj.Id = objects.RecordID(aliasID)
	m.Objects[aliasID] = &copyObj
	return nil
}

func (m *MockDatabase) ResolveObjectAlias(_ context.Context, aliasID string) (string, error) {
	if _, ok := m.Objects[aliasID]; ok {
		return aliasID, nil
	}
	return "", faults.ErrNotFound
}

func (m *MockDatabase) GetObjectsByChecksum(_ context.Context, checksum string) ([]objects.Record, error) {
	out := make([]objects.Record, 0)
	for id, obj := range m.Objects {
		if id == checksum || string(obj.Id) == checksum || hasChecksum(obj, checksum) {
			out = append(out, *m.copyObject(id, obj))
		}
	}
	return out, nil
}

func (m *MockDatabase) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]objects.Record, error) {
	out := make(map[string][]objects.Record, len(checksums))
	for _, checksum := range checksums {
		matches, err := m.GetObjectsByChecksum(ctx, checksum)
		if err != nil {
			return nil, err
		}
		out[checksum] = matches
	}
	return out, nil
}

func (m *MockDatabase) ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error) {
	out := make(map[string][]string, len(checksums))
	for _, checksum := range checksums {
		matches, err := m.GetObjectsByChecksum(ctx, checksum)
		if err != nil {
			return nil, err
		}
		for _, obj := range matches {
			if objectMatchesScope(obj.Authorizations, organization, project) {
				out[checksum] = append(out[checksum], string(obj.Id))
			}
		}
	}
	return out, nil
}

func (m *MockDatabase) ListObjectIDsByScope(_ context.Context, organization, project string) ([]string, error) {
	ids := make([]string, 0)
	for id, obj := range m.Objects {
		if objectMatchesScope(m.ObjectAuthz[id], organization, project) || objectMatchesScope(obj.Authorizations, organization, project) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (m *MockDatabase) GetS3Credential(_ context.Context, bucket string) (*buckets.Credential, error) {
	for key, credential := range m.Credentials {
		if key == bucket || strings.EqualFold(credential.Bucket, bucket) || strings.EqualFold(credential.CredentialID, bucket) {
			copyCredential := credential
			return &copyCredential, nil
		}
	}
	if m.NoDefaultCreds {
		return nil, nil
	}
	return &buckets.Credential{CredentialID: bucket, Bucket: bucket, Provider: "s3", Region: "us-east-1", AccessKey: "test-key", SecretKey: "test-secret"}, nil
}

func (m *MockDatabase) SaveS3Credential(_ context.Context, credential *buckets.Credential) error {
	if m.Credentials == nil {
		m.Credentials = map[string]buckets.Credential{}
	}
	key := strings.TrimSpace(credential.CredentialID)
	if key == "" {
		key = strings.TrimSpace(credential.Bucket)
	}
	m.Credentials[key] = *credential
	return nil
}

func (m *MockDatabase) DeleteS3Credential(_ context.Context, bucket string) error {
	delete(m.Credentials, bucket)
	return nil
}

func (m *MockDatabase) ListS3Credentials(_ context.Context) ([]buckets.Credential, error) {
	if len(m.Credentials) > 0 {
		out := make([]buckets.Credential, 0, len(m.Credentials))
		for _, credential := range m.Credentials {
			out = append(out, credential)
		}
		return out, nil
	}
	if m.NoDefaultCreds {
		return []buckets.Credential{}, nil
	}
	return []buckets.Credential{{CredentialID: "test-bucket-1", Bucket: "test-bucket-1", Provider: "s3", Region: "us-east-1"}}, nil
}

func bucketScopeKey(organization, project string) string {
	return strings.TrimSpace(organization) + "|" + strings.TrimSpace(project)
}

func (m *MockDatabase) CreateBucketScope(_ context.Context, scope *buckets.Scope) error {
	if m.BucketScopes == nil {
		m.BucketScopes = map[string]buckets.Scope{}
	}
	m.BucketScopes[bucketScopeKey(scope.Organization, scope.ProjectID)] = *scope
	return nil
}

func (m *MockDatabase) DeleteBucketScope(_ context.Context, organization, projectID, credentialID, pathPrefix string) error {
	key := bucketScopeKey(organization, projectID)
	scope, ok := m.BucketScopes[key]
	if !ok || (credentialID != "" && scope.CredentialID != credentialID && scope.Bucket != credentialID) || strings.Trim(scope.PathPrefix, "/") != strings.Trim(pathPrefix, "/") {
		return faults.ErrNotFound
	}
	delete(m.BucketScopes, key)
	return nil
}

func (m *MockDatabase) GetBucketScope(_ context.Context, organization, project string) (*buckets.Scope, error) {
	m.GetBucketScopeCalls++
	scope, ok := m.BucketScopes[bucketScopeKey(organization, project)]
	if !ok {
		return nil, faults.ErrNotFound
	}
	copyScope := scope
	return &copyScope, nil
}

func (m *MockDatabase) ListBucketScopes(_ context.Context) ([]buckets.Scope, error) {
	out := make([]buckets.Scope, 0, len(m.BucketScopes))
	for _, scope := range m.BucketScopes {
		out = append(out, scope)
	}
	return out, nil
}

func (m *MockDatabase) SavePendingLFSMeta(_ context.Context, entries []transfers.PendingMetadata) error {
	if m.PendingMeta == nil {
		m.PendingMeta = map[string]transfers.PendingMetadata{}
	}
	for _, entry := range entries {
		m.PendingMeta[entry.OID] = entry
	}
	return nil
}

func (m *MockDatabase) GetPendingLFSMeta(_ context.Context, oid string) (*transfers.PendingMetadata, error) {
	entry, ok := m.PendingMeta[oid]
	if !ok {
		return nil, faults.ErrNotFound
	}
	return &entry, nil
}

func (m *MockDatabase) PopPendingLFSMeta(ctx context.Context, oid string) (*transfers.PendingMetadata, error) {
	entry, err := m.GetPendingLFSMeta(ctx, oid)
	if err != nil {
		return nil, err
	}
	delete(m.PendingMeta, oid)
	return entry, nil
}

func (m *MockDatabase) RecordFileUpload(_ context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = map[string]usage.FileUsage{}
	}
	current := m.Usage[objectID]
	current.ObjectID = objectID
	current.UploadCount++
	now := time.Now().UTC()
	current.LastUploadTime = &now
	current.LastAccessTime = &now
	if obj := m.Objects[objectID]; obj != nil {
		current.Name = stringValue(obj.Name)
		current.Size = obj.Size
	}
	m.Usage[objectID] = current
	return nil
}

func (m *MockDatabase) RecordFileDownload(_ context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = map[string]usage.FileUsage{}
	}
	current := m.Usage[objectID]
	current.ObjectID = objectID
	current.DownloadCount++
	now := time.Now().UTC()
	current.LastDownloadTime = &now
	current.LastAccessTime = &now
	if obj := m.Objects[objectID]; obj != nil {
		current.Name = stringValue(obj.Name)
		current.Size = obj.Size
	}
	m.Usage[objectID] = current
	return nil
}

func (m *MockDatabase) RecordTransferAttributionEvents(_ context.Context, events []usage.Event) error {
	m.TransferEvents = append(m.TransferEvents, events...)
	return nil
}

func (m *MockDatabase) copyObject(id string, obj *objects.Record) *objects.Record {
	copyObj := *obj
	if authz, ok := m.ObjectAuthz[id]; ok {
		copyObj.Authorizations = cloneAuthzMap(authz)
	}
	return &copyObj
}

func hasChecksum(obj *objects.Record, checksum string) bool {
	for _, candidate := range obj.Checksums {
		if strings.EqualFold(strings.TrimSpace(candidate.Checksum), strings.TrimSpace(checksum)) {
			return true
		}
	}
	return false
}

func objectMatchesScope(authz map[string][]string, organization, project string) bool {
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

func cloneAuthzMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for organization, projects := range in {
		out[organization] = append([]string(nil), projects...)
	}
	return out
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

var (
	_ objects.RecordReader       = (*MockDatabase)(nil)
	_ objects.RecordWriter       = (*MockDatabase)(nil)
	_ objects.AccessMethodWriter = (*MockDatabase)(nil)
	_ objects.AccessPolicyWriter = (*MockDatabase)(nil)
	_ objects.AliasStore         = (*MockDatabase)(nil)
	_ objects.ContentReader      = (*MockDatabase)(nil)
	_ objects.ChecksumScopeQuery = (*MockDatabase)(nil)
	_ objects.ScopeQuery         = (*MockDatabase)(nil)
	_ buckets.CredentialReader   = (*MockDatabase)(nil)
	_ buckets.CredentialAdmin    = (*MockDatabase)(nil)
	_ buckets.ScopeStore         = (*MockDatabase)(nil)
	_ transfers.PendingStore     = (*MockDatabase)(nil)
	_ transfers.EventRecorder    = (*MockDatabase)(nil)
	_ usage.FileCounterRecorder  = (*MockDatabase)(nil)
)
