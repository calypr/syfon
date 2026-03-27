# Quickstart

This is the canonical local startup flow for `drs-server`.

Need software build or code generation instructions? See [Build](build.md).

## Prerequisites

- Go 1.26+
- SQLite3 CLI (`sqlite3`)
- Git

## 1. Clone and enter the repository

```bash
git clone <your-repo-url>
cd drs-server
```

## 2. Create local config

Create `config.local.yaml`:

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

## 3. Initialize SQLite schema

```bash
./db/scripts/init_sqlite_db.sh drs_local.db
```

## 4. Start the server

```bash
go run . serve --config config.local.yaml
```

## 5. Smoke test

```bash
curl -s http://localhost:8080/healthz
```

## Useful Endpoints

- `GET /healthz`
- `GET /index/swagger`
- `GET /index/openapi.yaml`
- `GET /service-info`
- `GET /index/{id}`
- `POST /index/bulk/sha256/validity`
- `GET /download/{id}`

## Local Gen3 Mock Auth (No Fence/Arborist)

Use this for local integration testing of Gen3 authorization behavior without deploying external auth services.

```bash
DRS_AUTH_MODE=gen3 \
DRS_AUTH_MOCK_ENABLED=true \
DRS_AUTH_MOCK_RESOURCES="/data_file,/programs/cbds/projects/end_to_end_test" \
DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete" \
go run . serve --config config.local.yaml
```

Optional toggles:

- `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER=true` requires an `Authorization` header before mock privileges are injected.
- If `DRS_AUTH_MOCK_RESOURCES` is omitted, default is `/data_file`.
- If `DRS_AUTH_MOCK_METHODS` is omitted, default is `*`.

## Mode Rules

- `auth.mode` is required and must be `local` or `gen3`.
- Local development should use `auth.mode: local` and SQLite.
- `gen3` mode is for deployed environments and requires PostgreSQL unless mock auth is enabled for local testing.