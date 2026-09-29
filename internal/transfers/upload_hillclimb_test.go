package transfers

import (
	"context"
	"fmt"
	"testing"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
)

func TestHillclimbScopedUploadBulkProfile(t *testing.T) {
	for _, count := range []int{1, 100} {
		for sample := 1; sample <= 3; sample++ {
			objectErrors := make(map[string]error, count)
			requests := make([]UploadRequest, count)
			for i := range requests {
				id := fmt.Sprintf("upload-%03d", i)
				objectErrors[id] = errorapi.ErrObjectNotFound
				requests[i] = UploadRequest{ObjectID: id, Key: id, Scope: &AccessScope{Organization: "org", Project: "project"}}
			}
			lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
				"bucket": {Provider: "s3", Bucket: "bucket", AccessKey: "access"},
			}}
			provider := &bulkAccessProvider{}
			service := bulkAccessService(t, &bulkObjectsFake{errors: objectErrors}, lookup, provider, &eventFake{})
			scopes := &countingScopeReader{
				scopes: map[string]buckets.Scope{
					"org|":        {Organization: "org", Bucket: "bucket", PathPrefix: "org-prefix"},
					"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "project-prefix"},
				},
				calls: make(map[string]int),
			}
			service.scopes = scopes
			session := access.NewSession("local")
			session.AuthzEnforced = true
			session.SetAuthorizations(nil, map[string]map[string]bool{
				"/organization/org/project/project": {"create": true},
			}, true)
			results := service.UploadBulk(access.WithSession(context.Background(), session), requests)
			if len(results) != count || len(provider.requests) != count {
				t.Fatalf("items=%d sample=%d results=%d signed=%d", count, sample, len(results), len(provider.requests))
			}
			for i, result := range results {
				wantKey := "org-prefix/project-prefix/" + requests[i].Key
				if result.Err != nil || result.ObjectID != requests[i].ObjectID || result.Existing || result.Target.Key != wantKey || result.URL != "signed-"+wantKey {
					t.Fatalf("items=%d sample=%d result[%d]=%+v", count, sample, i, result)
				}
			}
			credentialReads := len(lookup.calls)
			orgReads, projectReads := scopes.calls["org|"], scopes.calls["org|project"]
			if credentialReads < 1 || credentialReads > count || orgReads < 1 || orgReads > count || projectReads < 1 || projectReads > count {
				t.Fatalf("items=%d sample=%d credential=%d org=%d project=%d", count, sample, credentialReads, orgReads, projectReads)
			}
			t.Logf("upload_profile items=%d sample=%d credential_reads=%d org_scope_reads=%d project_scope_reads=%d", count, sample, credentialReads, orgReads, projectReads)
		}
	}
}
