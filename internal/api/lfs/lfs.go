package lfs

import (
	"sync"

	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
)

var (
	limitMu            sync.Mutex
	requestWindowMap   = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
)

func DefaultOptions() Options {
	return Options{
		MaxBatchObjects:              1000,
		MaxBatchBodyBytes:            10 * 1024 * 1024,
		RequestLimitPerMinute:        1200,
		BandwidthLimitBytesPerMinute: 0,
	}
}

func RegisterLFSRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager, opts ...Options) {
	effective := DefaultOptions()
	if len(opts) > 0 {
		effective = opts[0]
	}
	router.HandleFunc(config.RouteLFSBatch, handleBatch(database, uM, effective)).Methods("POST")
	router.HandleFunc(config.RouteLFSMetadata, handleMetadata(database)).Methods("POST")
	router.HandleFunc(config.RouteLFSObject, handleUploadProxy(database, uM)).Methods("PUT")
	router.HandleFunc(config.RouteLFSVerify, handleVerify(database)).Methods("POST")
}

func resetLFSLimitersForTest() {
	limitMu.Lock()
	defer limitMu.Unlock()
	requestWindowMap = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
}
