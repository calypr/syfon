package objects_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/objects"
)

type hillclimbRegistrationStore struct {
	objects.ObjectStore
	written       bool
	singleReads   int
	bulkReads     int
	bulkReadItems int
}

func (s *hillclimbRegistrationStore) RegisterObjects(ctx context.Context, records []drs.DrsObject) error {
	if err := s.ObjectStore.RegisterObjects(ctx, records); err != nil {
		return err
	}
	s.written = true
	return nil
}

func (s *hillclimbRegistrationStore) GetObject(ctx context.Context, id string) (*drs.DrsObject, error) {
	if s.written {
		s.singleReads++
	}
	return s.ObjectStore.GetObject(ctx, id)
}

func (s *hillclimbRegistrationStore) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	if s.written {
		s.bulkReads++
		s.bulkReadItems += len(ids)
	}
	return s.ObjectStore.GetBulkObjects(ctx, ids)
}

func TestHillclimbRegistrationReadbackProfile(t *testing.T) {
	for _, count := range []int{1, 100} {
		for sample := 1; sample <= 3; sample++ {
			store := &hillclimbRegistrationStore{ObjectStore: newSQLiteDatabase(t)}
			service := objects.NewService(store)
			records := make([]drs.DrsObject, count)
			for i := range records {
				id := fmt.Sprintf("registered-%03d", i)
				url := "s3://bucket/" + id
				records[i] = drs.DrsObject{
					Id: id, Checksums: []drs.Checksum{{Type: "sha256", Checksum: fmt.Sprintf("%064x", i+1)}},
					AccessMethods: &[]drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: url}}},
				}
			}
			returned, err := service.RegisterObjects(context.Background(), records)
			if err != nil || len(returned) != count {
				t.Fatalf("count=%d sample=%d returned=%d error=%v", count, sample, len(returned), err)
			}
			for i, record := range returned {
				if record.Id != records[i].Id || record.AccessMethods == nil || len(*record.AccessMethods) != 1 {
					t.Fatalf("count=%d sample=%d item=%d record=%+v", count, sample, i, record)
				}
			}
			if store.singleReads+store.bulkReads < 1 || store.singleReads+store.bulkReads > count {
				t.Fatalf("count=%d sample=%d singles=%d batches=%d", count, sample, store.singleReads, store.bulkReads)
			}
			t.Logf("register_profile items=%d sample=%d readback_singles=%d readback_batches=%d batch_items=%d", count, sample, store.singleReads, store.bulkReads, store.bulkReadItems)
		}
	}
}
