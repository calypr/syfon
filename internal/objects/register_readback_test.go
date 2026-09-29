package objects_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/objects"
)

type scriptedReadbackStore struct {
	*objectTestStore
	bulk           []drs.DrsObject
	bulkErr        error
	aliases        map[string]string
	aliasErr       error
	bulkRequested  []string
	aliasRequested []string
}

func (s *scriptedReadbackStore) GetBulkObjects(_ context.Context, ids []string) ([]drs.DrsObject, error) {
	s.bulkRequested = append([]string(nil), ids...)
	return s.bulk, s.bulkErr
}

func (s *scriptedReadbackStore) ResolveObjectAliases(_ context.Context, ids []string) (map[string]string, error) {
	s.aliasRequested = append([]string(nil), ids...)
	return s.aliases, s.aliasErr
}

func TestRegisterObjectsReadbackPreservesDuplicateAliasOrderAndPhysicalPrecedence(t *testing.T) {
	store := &scriptedReadbackStore{
		objectTestStore: &objectTestStore{Objects: make(map[string]*drs.DrsObject)},
		bulk: []drs.DrsObject{
			{Id: "physical", Name: ptr("durable physical")},
			{Id: "canonical", Name: ptr("durable canonical")},
		},
		aliases: map[string]string{"physical": "canonical", "legacy": "canonical"},
	}
	inputs := []drs.DrsObject{{Id: "physical", Name: ptr("submitted")}, {Id: "legacy"}, {Id: "physical"}}
	got, err := objects.NewService(store).RegisterObjects(context.Background(), inputs)
	if err != nil {
		t.Fatalf("RegisterObjects() error=%v", err)
	}
	if len(got) != 3 || got[0].Id != "physical" || got[1].Id != "canonical" || got[2].Id != "physical" ||
		got[0].Name == nil || *got[0].Name != "durable physical" || got[1].Name == nil || *got[1].Name != "durable canonical" {
		t.Fatalf("durable response=%+v", got)
	}
	if !reflect.DeepEqual(store.bulkRequested, []string{"physical", "legacy"}) || !reflect.DeepEqual(store.aliasRequested, []string{"legacy"}) {
		t.Fatalf("readback requests bulk=%v aliases=%v", store.bulkRequested, store.aliasRequested)
	}
}

func TestRegisterObjectsReadbackPropagatesMissingAndBatchFailures(t *testing.T) {
	bulkErr := errors.New("bulk read failed")
	aliasErr := errors.New("alias read failed")
	for _, test := range []struct {
		name     string
		bulk     []drs.DrsObject
		bulkErr  error
		aliases  map[string]string
		aliasErr error
		want     error
	}{
		{name: "missing durable row", want: errorapi.ErrObjectNotFound},
		{name: "bulk failure", bulkErr: bulkErr, want: bulkErr},
		{name: "alias failure", bulk: []drs.DrsObject{{Id: "canonical"}}, aliasErr: aliasErr, want: aliasErr},
		{name: "alias points to missing row", aliases: map[string]string{"submitted": "canonical"}, want: errorapi.ErrObjectNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &scriptedReadbackStore{
				objectTestStore: &objectTestStore{Objects: make(map[string]*drs.DrsObject)},
				bulk:            test.bulk, bulkErr: test.bulkErr, aliases: test.aliases, aliasErr: test.aliasErr,
			}
			_, err := objects.NewService(store).RegisterObjects(context.Background(), []drs.DrsObject{{Id: "submitted"}})
			if !errors.Is(err, test.want) {
				t.Fatalf("RegisterObjects() error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestRegisterObjectsReadbackReturnsDurableCanonicalDuplicates(t *testing.T) {
	database := newSQLiteDatabase(t)
	service := objects.NewService(database)
	checksum := strings.Repeat("a", 64)
	makeRecord := func(id, hash string) drs.DrsObject {
		return drs.DrsObject{
			Id: id, Checksums: []drs.Checksum{{Type: "sha256", Checksum: hash}},
			AccessMethods: &[]drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://bucket/" + id}}},
		}
	}
	if err := database.RegisterObjects(context.Background(), []drs.DrsObject{makeRecord("canonical", checksum)}); err != nil {
		t.Fatalf("seed canonical: %v", err)
	}
	inputs := []drs.DrsObject{
		makeRecord("first-alias", checksum), makeRecord("distinct", strings.Repeat("b", 64)), makeRecord("second-alias", checksum),
	}
	got, err := service.RegisterObjects(context.Background(), inputs)
	if err != nil {
		t.Fatalf("RegisterObjects() error=%v", err)
	}
	if len(got) != 3 || got[0].Id != "canonical" || got[1].Id != "distinct" || got[2].Id != "canonical" {
		t.Fatalf("durable response order=%+v", got)
	}
	for _, record := range []drs.DrsObject{got[0], got[2]} {
		if record.AccessMethods == nil || len(*record.AccessMethods) != 3 {
			t.Fatalf("canonical record lost merged locations: %+v", record)
		}
	}
}
