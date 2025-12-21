package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/calypr/drs-server/internal-go-server/util/mw"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	cfg := zap.NewProductionConfig()
	if *debug {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	log, _ := cfg.Build()
	defer func() { _ = log.Sync() }()

	r := chi.NewRouter()
	r.Use(mw.RequestLogger(log))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("listening", zap.String("addr", *addr))
	_ = srv.ListenAndServe()
}
