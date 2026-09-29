package sqlite

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/credentialcipher"
	"github.com/calypr/syfon/internal/persistence/store"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/s3"
	"github.com/calypr/syfon/internal/transfers"
)

type signingCountingDialect struct {
	store.Dialect
	queries    map[string]int
	signatures map[string]int
}

func (d *signingCountingDialect) Rebind(query string) string {
	q := strings.ToLower(query)
	category := "other"
	switch {
	case strings.Contains(q, "from bucket_scope"):
		category = "scope read"
	case strings.Contains(q, "from s3_credential"):
		category = "credential read"
	case strings.Contains(q, "transfer_attribution_event") || strings.Contains(q, "access_grant"):
		category = "attribution write"
	case strings.Contains(q, "drs_object"):
		category = "object read"
	}
	d.queries[category]++
	if d.signatures != nil {
		fields := strings.Fields(q)
		if len(fields) > 9 {
			fields = fields[:9]
		}
		d.signatures[category+": "+strings.Join(fields, " ")]++
	}
	return d.Dialect.Rebind(query)
}

func TestSigningProfile(t *testing.T) {
	for _, size := range []int{7, 100} {
		t.Run(fmt.Sprintf("items_%d", size), func(t *testing.T) {
			profileSigningSize(t, size)
		})
	}
}

func profileSigningSize(t *testing.T, size int) {
	const resource = "/organization/profile-org/project/profile-project"
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "profile.db")
	cipher, err := credentialcipher.New(credentialcipher.Config{SQLiteFile: path})
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewSqliteDB(path, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	credential := &buckets.Credential{CredentialID: "profile-cred", Bucket: "profile-bucket", Provider: "s3", Region: "us-east-1", AccessKey: "synthetic-access", SecretKey: "synthetic-secret", Endpoint: "http://127.0.0.1:1"}
	if err := base.SaveS3Credential(ctx, credential); err != nil {
		t.Fatal(err)
	}
	if err := base.CreateBucketScope(ctx, &buckets.Scope{Organization: "profile-org", ProjectID: "profile-project", CredentialID: credential.CredentialID, Bucket: credential.Bucket}); err != nil {
		t.Fatal(err)
	}

	uuids := make([]string, size)
	shas := make([]string, size)
	rows := make([]drs.DrsObject, size)
	for i := range rows {
		uuids[i] = fmt.Sprintf("profile-uuid-%03d", i)
		shas[i] = fmt.Sprintf("%x", sha256.Sum256([]byte(uuids[i])))
		methodID := "s3"
		rows[i] = drs.DrsObject{
			Id:               uuids[i],
			Checksums:        []drs.Checksum{{Type: "sha256", Checksum: shas[i]}},
			ControlledAccess: &[]string{resource},
			AccessMethods:    &[]drs.AccessMethod{{AccessId: &methodID, Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://profile-bucket/" + uuids[i]}}},
		}
	}
	if err := base.RegisterObjects(ctx, rows); err != nil {
		t.Fatal(err)
	}
	dialect := &signingCountingDialect{Dialect: sqliteDialect{}, queries: map[string]int{}}
	observed, err := store.OpenPrepared(ctx, base.DB(), dialect, cipher)
	if err != nil {
		t.Fatal(err)
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{Credentials: observed, CredentialAdmin: observed, Scopes: observed, Visibility: observed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := storage.NewManager(bucketService, s3.New())
	if err != nil {
		t.Fatal(err)
	}
	service := transfers.NewService(transfers.Dependencies{Objects: objects.NewService(observed), Storage: manager, Scopes: bucketService, Events: observed})
	session := access.NewSession("local")
	session.SetAuthorizations(nil, map[string]map[string]bool{resource: {"read": true}}, true)
	ctx = access.WithSession(ctx, session)
	for _, identity := range []struct {
		name string
		ids  []string
	}{{"uuid", uuids}, {"sha", shas}} {
		requests := make([]transfers.AccessLookupRequest, len(identity.ids))
		for i, id := range identity.ids {
			requests[i] = transfers.AccessLookupRequest{ObjectID: id, AccessID: "s3"}
		}
		// One warm run creates the signer client. Sample subsequent request batches.
		warm := service.IssueAccessBulk(ctx, requests)
		if len(warm.Resolved) != size || len(warm.Failures) != 0 {
			t.Fatalf("%s warm: resolved=%d failures=%v", identity.name, len(warm.Resolved), warm.Failures)
		}
		var timings []time.Duration
		var counts map[string]int
		for sample := 0; sample < 3; sample++ {
			dialect.queries = map[string]int{}
			dialect.signatures = map[string]int{}
			start := time.Now()
			got := service.IssueAccessBulk(ctx, requests)
			timings = append(timings, time.Since(start))
			if len(got.Resolved) != size || len(got.Failures) != 0 {
				t.Fatalf("%s sample: resolved=%d failures=%v", identity.name, len(got.Resolved), got.Failures)
			}
			counts = dialect.queries
		}
		sort.Slice(timings, func(i, j int) bool { return timings[i] < timings[j] })
		total := 0
		for _, n := range counts {
			total += n
		}
		fmt.Fprintf(os.Stdout, "PROFILE size=%d identity=%s median=%s samples=%v counted=%d categories=%v\n", size, identity.name, timings[1], timings, total, counts)
		if size == 7 && identity.name == "sha" {
			fmt.Fprintf(os.Stdout, "PROFILE signatures=%v\n", dialect.signatures)
		}
	}
	var events int
	if err := base.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM transfer_attribution_event").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 8*size {
		t.Fatalf("recorded %d access events, want %d", events, 8*size)
	}
	fmt.Fprintf(os.Stdout, "PROFILE size=%d event_rows=%d\n", size, events)
}
