package sqlite

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/store"
)

type overwriteAliasDialect struct {
	store.Dialect
	single           int
	batch            int
	resolutionSingle int
	resolutionBatch  int
}

func (d *overwriteAliasDialect) Rebind(query string) string {
	q := strings.ToLower(query)
	if strings.Contains(q, "from drs_object_alias") {
		if strings.Contains(q, "alias_id = ?") {
			d.single++
		} else {
			d.batch++
		}
		pcs := make([]uintptr, 24)
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, "(*Store).ResolveObjectAlias") {
				d.resolutionSingle++
				break
			}
			if strings.HasSuffix(frame.Function, "(*Store).ResolveObjectAliases") {
				d.resolutionBatch++
				break
			}
			if !more {
				break
			}
		}
	}
	return d.Dialect.Rebind(query)
}

func TestBulkOverwriteAliasProfile(t *testing.T) {
	const resource = "/organization/profile-org/project/profile-project"
	for _, size := range []int{1, 100} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			var samples []time.Duration
			var singles, batches, resolutionSingle, resolutionBatch int
			for run := 0; run < 3; run++ {
				ctx := context.Background()
				base, err := NewSqliteDB(t.TempDir()+"/overwrite.db", nil)
				if err != nil {
					t.Fatal(err)
				}
				candidates := make([]drs.DrsObject, size)
				for i := range candidates {
					id := fmt.Sprintf("object-%03d", i)
					sha := fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
					candidates[i] = drs.DrsObject{Id: id, Checksums: []drs.Checksum{{Type: "sha256", Checksum: sha}}, ControlledAccess: &[]string{resource}}
					if i%2 == 0 {
						if err := base.RegisterObjects(ctx, []drs.DrsObject{candidates[i]}); err != nil {
							t.Fatal(err)
						}
					}
				}
				dialect := &overwriteAliasDialect{Dialect: sqliteDialect{}}
				observed, err := store.OpenPrepared(ctx, base.DB(), dialect, nil)
				if err != nil {
					t.Fatal(err)
				}
				session := access.NewSession("local")
				session.SetAuthorizations(nil, map[string]map[string]bool{resource: {"create": true, "update": true}}, true)
				ctx = access.WithSession(ctx, session)
				start := time.Now()
				result, err := objects.NewService(observed).BulkOverwriteObjects(ctx, "profile-org", "profile-project", candidates)
				samples = append(samples, time.Since(start))
				if err != nil {
					t.Fatal(err)
				}
				if result.Created != size/2 || result.Replaced != (size+1)/2 {
					t.Fatalf("result=%+v", result)
				}
				singles, batches, resolutionSingle, resolutionBatch = dialect.single, dialect.batch, dialect.resolutionSingle, dialect.resolutionBatch
				if err := base.Close(); err != nil {
					t.Fatal(err)
				}
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			fmt.Fprintf(os.Stdout, "OVERWRITE_ALIAS_PROFILE size=%d median=%s samples=%v single=%d batch=%d resolution_single=%d resolution_batch=%d\n", size, samples[1], samples, singles, batches, resolutionSingle, resolutionBatch)
		})
	}
}
