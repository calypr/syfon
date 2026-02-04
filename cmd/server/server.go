package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/db/postgres"
	"github.com/calypr/drs-server/db/sqlite"
	"github.com/calypr/drs-server/internal/api/admin"
	"github.com/calypr/drs-server/internal/api/fence"
	"github.com/calypr/drs-server/internal/api/gen3"
	"github.com/calypr/drs-server/service"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/spf13/cobra"
)

var configFile string

var Cmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the DRS Object API server",
	Run: func(cmd *cobra.Command, args []string) {
		// Load Config
		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}

		// Init DB
		var database core.DatabaseInterface
		var errDb error

		if cfg.Database.Sqlite != nil {
			dbPath := cfg.Database.Sqlite.File
			if dbPath == "" {
				dbPath = "drs.db"
			}
			fmt.Printf("Initializing SqliteDB (File: %s)\n", dbPath)
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
			fmt.Printf("Initializing PostgresDB (Host: %s, DB: %s)\n", cfg.Database.Postgres.Host, cfg.Database.Postgres.Database)
			database, errDb = postgres.NewPostgresDB(dsn)
		} else {
			log.Fatal("No database configuration provided")
		}

		if errDb != nil {
			log.Fatalf("Failed to initialize database: %v", errDb)
		}

		// Load S3 Credentials from Config if present
		if len(cfg.S3Credentials) > 0 {
			fmt.Printf("Loading %d S3 credentials from config\n", len(cfg.S3Credentials))
			for _, c := range cfg.S3Credentials {
				cred := &core.S3Credential{
					Bucket:    c.Bucket,
					Region:    c.Region,
					AccessKey: c.AccessKey,
					SecretKey: c.SecretKey,
					Endpoint:  c.Endpoint,
				}
				if err := database.SaveS3Credential(cmd.Context(), cred); err != nil {
					log.Printf("Failed to save credential for bucket %s: %v", c.Bucket, err)
				}
			}
		}

		// Init UrlManager
		uM := urlmanager.NewS3UrlManager(database)

		// Init Service
		service := service.NewObjectsAPIService(database, uM)

		// Init Controller
		objectsController := drs.NewObjectsAPIController(service)
		serviceInfoController := drs.NewServiceInfoAPIController(service)

		// Init Router
		router := drs.NewRouter(objectsController, serviceInfoController)
		router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		// Register Admin Routes
		admin.RegisterAdminRoutes(router, database, uM)

		// Register Gen3 Compatibility Routes (for git-drs)
		gen3.RegisterGen3Routes(router, database)

		// Register Fence Compatibility Routes (for data download/upload)
		fence.RegisterFenceRoutes(router, database, uM)

		addr := fmt.Sprintf(":%d", cfg.Port)
		fmt.Printf("Server starting on %s\n", addr)
		log.Fatal(http.ListenAndServe(addr, router))
	},
}

func init() {
	Cmd.Flags().StringVar(&configFile, "config", "", "Path to configuration file (json/yaml)")
}
