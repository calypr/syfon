package server

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/version"
	"github.com/lib/pq"
)

func TestRetryProductionSchemaCheckOnlyRetriesTransientSchemaState(t *testing.T) {
	transient := []error{
		errors.Join(errors.New("failed to ping database"), &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
		errors.Join(errors.New("failed to ping database"), testPostgresError("08001")),
		errors.Join(errors.New("failed to ping database"), context.DeadlineExceeded),
		errors.New("schema migration ledger is missing; run the cluster DB-init Job"),
		errors.New("required schema relation \"drs_object\" is missing"),
		errors.New("database schema is behind supported version 2"),
	}
	for _, test := range transient {
		if !retryProductionSchemaCheck(test) {
			t.Errorf("retryProductionSchemaCheck(%v) = false, want true", test)
		}
	}
	permanent := []error{
		errors.Join(errors.New("failed to ping database"), testPostgresError("28P01")),
		errors.Join(errors.New("failed to ping database"), testPostgresError("42501")),
		errors.Join(errors.New("failed to ping database"), testPostgresError("3D000")),
		errors.New("failed to ping database: unclassified driver error"),
		errors.New("database schema migration version 3 is newer than this binary"),
		errors.New("schema migration 1 checksum or name mismatch"),
		errors.New("schema migration ledger has a gap before version 2"),
	}
	for _, test := range permanent {
		if retryProductionSchemaCheck(test) {
			t.Errorf("retryProductionSchemaCheck(%v) = true, want false", test)
		}
	}
}

func testPostgresError(code string) error {
	return &pq.Error{Code: pq.ErrorCode(code), Message: "synthetic startup error"}
}

func TestOpenPostgresDatabaseFailsFastOnAuthenticationFailure(t *testing.T) {
	dsn := os.Getenv("SYFON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SYFON_TEST_POSTGRES_DSN is not configured")
	}
	const rejectedPassword = "syfon-invalid-test-password"
	badDSN := dsn
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse test PostgreSQL DSN: %v", err)
		}
		username := ""
		if parsed.User != nil {
			username = parsed.User.Username()
		}
		parsed.User = url.UserPassword(username, rejectedPassword)
		badDSN = parsed.String()
	} else {
		badDSN += " password=" + rejectedPassword
	}
	parsedDSN, err := pq.NewConfig(badDSN)
	if err != nil {
		t.Fatalf("parse test PostgreSQL credentials: %v", err)
	}
	if parsedDSN.Password != rejectedPassword {
		t.Fatal("test DSN did not apply the deliberately rejected password")
	}

	cfg := &config.Config{Profile: config.ProfileProduction}
	cfg.Database.Postgres = &config.PostgresConfig{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	_, err = openPostgresDatabase(ctx, cfg, badDSN, nil)
	if err == nil {
		t.Fatal("PostgreSQL accepted the deliberately invalid password")
	}
	var sqlStateError interface{ SQLState() string }
	if !errors.As(err, &sqlStateError) || sqlStateError.SQLState() != "28P01" {
		t.Fatalf("authentication failure = %v, want SQLSTATE 28P01", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("permanent authentication failure retried for %s", elapsed)
	}
}

func TestServiceInfoUsesLinkerProvidedVersion(t *testing.T) {
	original := version.Version
	t.Cleanup(func() { version.Version = original })
	version.Version = "v9.8.7-test"

	info := serviceInfoForConfig(testServiceInfoConfig())
	if info.Version != "v9.8.7-test" {
		t.Fatalf("service version = %q, want linker-provided version", info.Version)
	}
	if info.Type.Version != "1.5.0" {
		t.Fatalf("DRS type version = %q, want 1.5.0", info.Type.Version)
	}
}

func testServiceInfoConfig() *config.Config {
	return &config.Config{
		Profile: config.ProfileDevelopment,
		Service: config.ServiceConfig{
			ID:              "drs-service-calypr",
			Name:            "Calypr DRS Server",
			Description:     "Calypr-backed DRS server",
			Environment:     "dev",
			Organization:    "Calypr",
			OrganizationURL: "https://github.com/calypr/syfon",
		},
		DRS:    config.DRSConfig{MaxBulkRequestLength: 100},
		Routes: config.RoutesConfig{Ga4gh: true},
	}
}

func TestServiceInfoForConfigUsesCompositeIdentityAndAdvertisedLimit(t *testing.T) {
	cfg := &config.Config{
		Profile: config.ProfileProduction,
		Service: config.ServiceConfig{
			ID:               "org.example.drs",
			Name:             "Example DRS",
			Description:      "Example service",
			Environment:      "prod",
			Organization:     "Example Org",
			OrganizationURL:  "https://example.org",
			ContactURL:       "https://example.org/contact",
			DocumentationURL: "https://example.org/docs",
		},
		DRS:    config.DRSConfig{MaxBulkRequestLength: 7},
		Routes: config.RoutesConfig{Ga4gh: true},
	}
	info := serviceInfoForConfig(cfg)
	if info.Id != "org.example.drs" || info.Type.Version != "1.5.0" || info.MaxBulkRequestLength != 7 {
		t.Fatalf("unexpected service identity: %+v", info)
	}
	if info.Organization.Name != "Example Org" || info.Organization.Url != "https://example.org" {
		t.Fatalf("unexpected organization: %+v", info.Organization)
	}
	if info.Drs == nil || info.Drs.MaxBulkRequestLength != 7 || info.Drs.ObjectRegistrationSupported == nil || !*info.Drs.ObjectRegistrationSupported {
		t.Fatalf("unexpected DRS capabilities: %+v", info.Drs)
	}
	if info.Drs.ObjectCount != nil || info.Drs.TotalObjectSize != nil {
		t.Fatal("service info fabricated object counts")
	}
}
