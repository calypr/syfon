<div align="center">
  <img src="docs/images/syfon-logo.png" alt="syfon logo" width="520" />
  <br><br>
  <a href="https://pkg.go.dev/github.com/calypr/syfon"><img src="https://pkg.go.dev/badge/github.com/calypr/syfon.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/calypr/syfon"><img src="https://goreportcard.com/badge/github.com/calypr/syfon" alt="Go Report Card"></a>
  <a href="https://github.com/calypr/syfon/actions/workflows/ci.yaml"><img src="https://github.com/calypr/syfon/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://app.codecov.io/gh/calypr/syfon/tree/development"><img src="https://codecov.io/gh/calypr/syfon/branch/development/graph/badge.svg" alt="Coverage"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"></a>
  <a href="https://github.com/calypr/syfon/releases"><img src="https://img.shields.io/github/v/release/calypr/syfon" alt="Latest Release"></a>
  <br><br>
  <p align="center">A lightweight, production-grade implementation of a GA4GH Data Repository Service (DRS) server in Go.</p>
</div>

# Quickstart

## 1. Install Syfon

```bash
curl -sSL https://calypr.org/syfon/install.sh | bash
```

## 2. Start Syfon Server

<details><summary><code>local.yaml</code></summary>

```yaml
port: 8080
auth:
  mode: local
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

</details>

```bash
syfon serve --config local.yaml
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

Record scope note:
- Syfon supports both scoped records (with `authz`, such as `/programs/<org>/projects/<project>`) and unscoped records (empty `authz`).
- Unscoped `ls` (`GET /index` without `organization/project/authz` filters) returns all records, including unscoped ones.
- RBAC checks still apply for scoped records in `gen3` mode.
- Recommended production policy: require project/authz at write time so unscoped records are not created unintentionally.

### Local Gen3 Mock Auth (no redeploy loop)

For local integration testing of Gen3 authorization behavior without Fence/Arborist and without PostgreSQL, run with mock auth:

```bash
DRS_AUTH_MODE=gen3 \
DRS_AUTH_MOCK_ENABLED=true \
DRS_AUTH_MOCK_RESOURCES="/data_file,/programs/cbds/projects/end_to_end_test" \
DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete" \
go run . serve --config local.yaml
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
go run . serve --config local.yaml
```

## Minio Starter Kit

Start up a docker container with MinIO for testing:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=admin" \
  -e "MINIO_ROOT_PASSWORD=password123" \
  -v ./data:/data \
  minio/minio server /data --console-address ":9001"
```

Create a config file called `local.yaml`

```yaml
port: 8080

auth:
    mode: local

database:
  sqlite:
    file: "drs.db"
database:
  sqlite:
    file: "drs.db"
s3_credentials:
  - bucket: "test-bucket"
    region: "us-east-1"
    access_key: "admin"
    secret_key: "password123"
    endpoint: "http://localhost:9000"
```

Start the syfon server
```
syfon server --config local.yaml
```

Upload a file
```
syfon upload --file README.md
```

List records
```
syfon ls
```

This test starts MinIO in Docker, starts a real syfon server configured against it, then verifies `ping`, `upload`, `download`, and `sha256sum`. It skips automatically when the opt-in flag is not set, and it also skips when Docker is unavailable.

# Architecture

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

# Development

The project uses a Makefile for common tasks:
- `make gen`: Generates the DRS server code from the official GA4GH OpenAPI spec (Git submodule).
- `make test`: Runs all unit and integration tests.
- `make test-unit`: Runs unit tests only (excludes integration packages).
- `make coverage`: Runs coverage for core production packages (db/service/middleware/url signing) and writes `coverage/coverage.out`, `coverage/coverage.txt`, and `coverage/coverage.html`.
- `make coverage-full`: Runs broader compatibility-layer coverage (includes internal compatibility and LFS packages).
- `make serve`: Starts the DRS server.

## apigen Scope (Current vs Future)

Syfon uses `oapi-codegen` to generate the HTTP boundary, but it does not use the stock generator output directly. The repo keeps a set of user templates that adapt the generated server to Fiber v3.

The pieces are:

- `apigen/api/*.openapi.yaml`: the source OpenAPI specs.
- `apigen/specs/oapi-*.yaml`: generator configs that select Fiber server generation and wire in local templates.
- `apigen/templates/fiber/*.tmpl`: Fiber-specific interface and middleware templates.
- `apigen/templates/strict/*.tmpl`: strict-handler templates that generate request/response wrapper types and strict server glue.
- `apigen/{drs,internalapi,lfsapi,metricsapi,bucketapi}`: generated packages committed to the repo.

What the templates do:

- `fiber-interface.tmpl` generates server interfaces that take `fiber.Ctx` plus path/query/header params.
- `fiber-middleware.tmpl` converts Fiber request data into typed params and calls the server interface.
- `strict-fiber.tmpl` generates strict-handler request objects and the request/response bridge used by `NewStrictHandler(...)`.
- `strict-fiber-interface.tmpl` generates the strict server interface and response object types.

Practical rule of thumb:

- Edit the OpenAPI spec when the API contract changes.
- Edit the templates when the Fiber binding behavior needs to change.
- Do not hand-edit generated code in `apigen/*`; regenerate it with `make gen` instead.

The runtime wiring then lives in `cmd/server` and `internal/api/*`, where the generated handlers are attached to Fiber routers.

## Running Integration Tests

You can run integration tests using your own config file:

```bash
go test ./cmd/server -v -count=1 -testConfig=example.yaml
```

Docker-backed MinIO upload and download coverage is available behind an opt-in flag:

```bash
SYFON_E2E_DOCKER=1 go test ./cmd -run TestSyfonDockerMinIOE2E -v -count=1
```

Docker-backed cloud emulator E2E suites are also available behind the same opt-in flag:

```bash
SYFON_E2E_DOCKER=1 go test ./cmd -run '^TestSyfonDockerFakeGCSE2E$' -v -count=1
SYFON_E2E_DOCKER=1 go test ./cmd -run '^TestSyfonDockerAzuriteE2E$' -v -count=1
```

To run all Docker-backed CLI E2E suites together (MinIO + fake-gcs-server + Azurite):

```bash
SYFON_E2E_DOCKER=1 go test ./cmd -run '^TestSyfonDocker(MinIOE2E|MultipartUpload|FakeGCSE2E|AzuriteE2E)$' -v -count=1
```

These suites are executed in CI with Docker; locally they will skip unless `SYFON_E2E_DOCKER=1` is set.

# License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
