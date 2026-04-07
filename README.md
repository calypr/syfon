<p align="center">
  <img src="docs/images/syfon-logo.png" alt="syfon logo" width="520" />
</p>

# syfon

A lightweight, production-grade implementation of a GA4GH Data Repository Service (DRS) server in Go.

## Quickstart

### Prerequisites

- Go 1.24+
- SQLite3 CLI (`sqlite3`)
- Git

### 1. Clone and enter the repo

```bash
git clone <your-repo-url>
cd syfon
```

### 2. Run tests

```bash
go test ./... -count=1
```

### 3. Start in local mode (SQLite, no gen3 authz)

Create `config.local.yaml`:

```yaml
port: 8080
auth:
  mode: local
  # optional basic auth for local mode
  # basic:
  #   username: "drs-user"
  #   password: "drs-pass"
database:
  sqlite:
    file: "drs_local.db"
s3_credentials:
  - bucket: "local-bucket"
    region: "us-east-1"
    access_key: "minio-user"
    secret_key: "minio-pass"
    endpoint: "http://localhost:9000"
```

Run:

```bash
./db/scripts/init_sqlite_db.sh drs_local.db
go run . serve --config config.local.yaml
```

Smoke test:

```bash
curl -s http://localhost:8080/healthz
```

Notes:
- `auth.mode` is required and must be `local` or `gen3`.
- Local development should run `auth.mode: local` with SQLite only.
- In `local` mode, set `auth.basic.username/password` (or env `DRS_BASIC_AUTH_USER` / `DRS_BASIC_AUTH_PASSWORD`) to enforce HTTP basic auth.
- `gen3` mode is for deployed environments and requires PostgreSQL.

### Local Gen3 Mock Auth (no redeploy loop)

For local integration testing of Gen3 authorization behavior without Fence/Arborist and without PostgreSQL, run with mock auth:

```bash
DRS_AUTH_MODE=gen3 \
DRS_AUTH_MOCK_ENABLED=true \
DRS_AUTH_MOCK_RESOURCES="/data_file,/programs/cbds/projects/end_to_end_test" \
DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete" \
go run . serve --config config.local.yaml
```

Optional:
- `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER=true` requires an `Authorization` header before mock privileges are injected.
- If `DRS_AUTH_MOCK_RESOURCES` is omitted, default is `"/data_file"`.
- If `DRS_AUTH_MOCK_METHODS` is omitted, default is `"*"` (full method access).

## Purpose

The `syfon` provides a robust implementation of the [GA4GH DRS API](https://ga4gh.github.io/data-repository-service-schemas/). It is designed to manage metadata for data objects and provide secure access via signed URLs.

### Key Features
- **GA4GH DRS Compliance**: Implements the standard DRS API for describing and accessing data objects.
- **Database Flexibility**: Supports both **SQLite** and **PostgreSQL** backends with a modular driver architecture.
- **S3 Integration**: Native support for Amazon S3 (and compatible storage like MinIO) with signed URL generation for downloads and multipart uploads.

## Configuration

The server is configured via a YAML or JSON file. Use the following structure to set up your environment:

```yaml
port: 8080
auth:
  mode: "local" # required: "local" or "gen3"
  # basic:
  #   username: "user"
  #   password: "pass"

database:
  sqlite:
    file: "drs.db"
  # Or use PostgreSQL:
  # postgres:
  #   host: "localhost"
  #   port: 5432
  #   user: "user"
  #   password: "password"
  #   database: "drs"
  #   sslmode: "disable"

s3_credentials:
  - bucket: "my-test-bucket"
    region: "us-east-1"
    access_key: "AKIAXXXXXXXXXXXXXXXX"
    secret_key: "SECRETKEY"
    endpoint: "s3.amazonaws.com" # Optional: set for MinIO or custom backends
```

In `gen3` mode, PostgreSQL is required unless `DRS_AUTH_MOCK_ENABLED=true` is set for local mock-auth testing.

Detailed configuration reference (including env overrides): [docs/configuration.md](docs/configuration.md)

## Gen3/PostgreSQL Schema Initialization

For deployment environments using PostgreSQL, schema initialization is managed by the Helm chart (`helm/syfon/templates/postgres-schema-configmap.yaml` + init Job).
This repository intentionally does not ship a separate Postgres init SQL script.

## Local Development Workflow

```bash
go test ./... -count=1
./db/scripts/init_sqlite_db.sh drs_local.db
go run . serve --config config.local.yaml
```

Useful endpoints:
- `GET /healthz`
- `GET /index/swagger` (Swagger UI)
- `GET /index/openapi.yaml` (OpenAPI spec)
- `GET /service-info`
- `GET /index/{id}` (gen3 compatibility)
- `POST /index/bulk/sha256/validity` (bulk sha validity map for git-lfs style flows)
- `GET /download/{id}` (fence compatibility)

## Running Integration Tests

You can run integration tests using your own config file:

```bash
go test ./cmd/server -v -count=1 -testConfig=config.yaml
```

## Architecture

The project follows a modular structure to ensure maintainability:
- `db/core`: Core interfaces and models.
- `db/sqlite`, `db/postgres`: Database implementation drivers.
- `internal/api`: Subpackages for different API contexts (Core, internal compatibility, LFS, metrics, docs, middleware).
- `service`: High-level business logic implementing the DRS service.
- `urlmanager`: Logic for interacting with cloud storage providers.

See DB table details and relationships in [db/README.md](db/README.md).

## Go Client SDK (Multi-Module)

This repository now includes a separate Go client module at `./client`:

- Module path: `github.com/calypr/syfon/client`
- Purpose: reusable HTTP client for Syfon APIs (used by the Syfon CLI and external tools)

Example import:

```go
import syclient "github.com/calypr/syfon/client"
```

The root module (`github.com/calypr/syfon`) uses a local `replace` during development:

```go
replace github.com/calypr/syfon/client => ./client
```

## Development

The project uses a Makefile for common tasks:
- `make gen`: Generates the DRS server code from the official GA4GH OpenAPI spec (Git submodule).
- `make test`: Runs all unit and integration tests.
- `make test-unit`: Runs unit tests only (excludes integration packages).
- `make coverage`: Runs coverage for core production packages (db/service/middleware/url signing) and writes `coverage/coverage.out`, `coverage/coverage.txt`, and `coverage/coverage.html`.
- `make coverage-full`: Runs broader compatibility-layer coverage (includes internal compatibility and LFS packages).
- `make serve`: Starts the DRS server.

### apigen Scope (Current vs Future)

The `apigen` module is currently used as a shared model/types package, not a full server/client operation generator. In practice, we generate and commit schemas/models from OpenAPI (`components/schemas`), while route handlers and request wiring are implemented manually under `internal/api/internaldrs` and related packages. This means path/operation updates in `apigen/api/*.openapi.yaml` may change contract/docs without producing new generated handler code.

This is intentional for now to keep control of runtime behavior and compatibility logic. We can expand `apigen` later to include operation-level generation (`apis`/server interfaces) once we decide to move more routing and handler contracts to generated code.
