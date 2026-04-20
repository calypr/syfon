package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/crypto"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/db/postgres"
	"github.com/calypr/syfon/internal/db/sqlite"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/signer/azure"
	"github.com/calypr/syfon/internal/signer/file"
	"github.com/calypr/syfon/internal/signer/gcs"
	"github.com/calypr/syfon/internal/signer/s3"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/spf13/cobra"
)

var configFile string

var Cmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the DRS Object API server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(logger)
		fatal := func(msg string, args ...any) {
			logger.Error(msg, args...)
			os.Exit(1)
		}

		// Load Config
		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			fatal("failed to load config", "err", err)
		}
		if cfg.Auth.Mode == config.AuthModeGen3 && cfg.Database.Postgres == nil && !isMockAuthEnabled() {
			fatal("auth.mode=gen3 requires postgres database")
		}

		// Init DB
		var database db.DatabaseInterface
		var errDb error

		if cfg.Database.Sqlite != nil {
			dbPath := cfg.Database.Sqlite.File
			if dbPath == "" {
				dbPath = "drs.db"
			}
			logger.Info("initializing sqlite database", "file", dbPath)
			database, errDb = sqlite.NewSqliteDB(dbPath)
		} else if cfg.Database.Postgres != nil {
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
				cfg.Database.Postgres.User,
				cfg.Database.Postgres.Password,
				cfg.Database.Postgres.Host,
				cfg.Database.Postgres.Port,
				cfg.Database.Postgres.Database,
				cfg.Database.Postgres.SSLMode,
			)
			logger.Info("initializing postgres database", "host", cfg.Database.Postgres.Host, "database", cfg.Database.Postgres.Database)
			database, errDb = postgres.NewPostgresDB(dsn)
		} else {
			fatal("no database configuration provided")
		}

		if errDb != nil {
			fatal("failed to initialize database", "err", errDb)
		}

		// Load S3 Credentials from Config if present
		if len(cfg.S3Credentials) > 0 {
			encryptionEnabled, encErr := crypto.CredentialEncryptionEnabled()
			if encErr != nil {
				fatal("invalid credential encryption configuration", "env", crypto.CredentialMasterKeyEnv, "err", encErr)
			}
			if !encryptionEnabled {
				fatal("s3 credential encryption key is required", "env", crypto.CredentialMasterKeyEnv)
			}

			logger.Info("loading configured s3 credentials", "count", len(cfg.S3Credentials))
			// S3 credentials are encrypted before persistence and audited on read/write/delete/list.
			for _, c := range cfg.S3Credentials {
				cred := &models.S3Credential{
					Bucket:    c.Bucket,
					Provider:  c.Provider,
					Region:    c.Region,
					AccessKey: c.AccessKey,
					SecretKey: c.SecretKey,
					Endpoint:  c.Endpoint,
				}
				if err := database.SaveS3Credential(cmd.Context(), cred); err != nil {
					logger.Error("failed to save s3 credential", "bucket", c.Bucket, "err", err)
				}
			}
		}

		// Init unified URL manager.
		needsUrlManager := cfg.Routes.Ga4gh || cfg.Routes.Internal || cfg.Routes.LFS
		var uM *urlmanager.Manager
		if needsUrlManager {
			uM = urlmanager.NewManager(database, cfg.Signing)
			uM.RegisterSigner(common.S3Provider, s3.NewS3Signer(database))
			uM.RegisterSigner(common.GCSProvider, gcs.NewGCSSigner(database))
			uM.RegisterSigner(common.AzureProvider, azure.NewAzureSigner(database))
			fSigner, fErr := file.NewFileSigner("/")
			if fErr == nil {
				uM.RegisterSigner(common.FileProvider, fSigner)
			} else {
				logger.Warn("failed to initialize file signer", "err", fErr)
			}
		}

		// Init unified Object Manager.
		om := core.NewObjectManager(database, uM)

		// Build Fiber runtime and middleware pipeline.
		app := fiber.New(fiber.Config{
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 120 * time.Second,
			IdleTimeout:  120 * time.Second,
			AppName:      "Syfon DRS Server",
		})
		app.Use(recover.New())

		// Init AuthZ Middleware
		// We use a standard slog.Logger for data-client compatibility
		slogLogger := logger
		authzMiddleware := middleware.NewAuthzMiddleware(
			slogLogger,
			cfg.Auth.Mode,
			cfg.Auth.Basic.Username,
			cfg.Auth.Basic.Password,
		)
		requestIDMiddleware := middleware.NewRequestIDMiddleware(slogLogger)

		rt := &serverRuntime{
			app:                 app,
			cfg:                 cfg,
			database:            database,
			om:                  om,
			uM:                  uM,
			authzMiddleware:     authzMiddleware,
			requestIDMiddleware: requestIDMiddleware,
		}
		applyServerOptions(rt, buildServerOptions(cfg)...)

		addr := fmt.Sprintf(":%d", cfg.Port)
		logger.Info("server starting", "addr", addr)

		errCh := make(chan error, 1)
		go func() {
			if err := app.Listen(addr); err != nil {
				errCh <- err
			}
		}()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigCh)

		select {
		case err := <-errCh:
			fatal("server listen failed", "err", err)
		case sig := <-sigCh:
			logger.Info("shutdown signal received", "signal", sig.String())
		case <-cmd.Context().Done():
			logger.Info("shutdown requested by context cancellation")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(shutdownCtx); err != nil {
			fatal("server shutdown failed", "err", err)
		}
		logger.Info("server shutdown complete")
	},
}

func init() {
	Cmd.Flags().StringVar(&configFile, "config", "", "Path to configuration file (json/yaml)")
}

func isMockAuthEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("DRS_AUTH_MOCK_ENABLED"))
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
