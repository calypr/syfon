package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
)

// InMemoryDB implements DatabaseInterface
var _ DatabaseInterface = (*InMemoryDB)(nil)

type InMemoryDB struct {
	mu      sync.RWMutex
	objects map[string]*drs.DrsObject
}

func NewInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		objects: make(map[string]*drs.DrsObject),
	}
}

func (db *InMemoryDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	return &drs.Service{
		Id:          "drs-service-1",
		Name:        "CalypR DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: "A simple DRS server",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Environment: "dev",
		Version:     "1.0.0",
	}, nil
}

func (db *InMemoryDB) GetObject(ctx context.Context, id string) (*drs.DrsObject, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if obj, ok := db.objects[id]; ok {
		return obj, nil
	}
	return nil, errors.New("object not found")
}

func (db *InMemoryDB) DeleteObject(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.objects[id]; !ok {
		return errors.New("object not found")
	}
	delete(db.objects, id)
	return nil
}

func (db *InMemoryDB) CreateObject(ctx context.Context, obj *drs.DrsObject) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.objects[obj.Id]; ok {
		return errors.New("object already exists")
	}
	db.objects[obj.Id] = obj
	return nil
}

func (db *InMemoryDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var results []drs.DrsObject
	for _, obj := range db.objects {
		for _, cs := range obj.Checksums {
			if cs.Checksum == checksum {
				results = append(results, *obj)
				break
			}
		}
	}
	return results, nil
}
