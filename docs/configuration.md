# Configuration

Configuration is provided via YAML (or JSON) and passed with `--config`.

## Base Configuration

```yaml
port: 8080
auth:
  mode: local # required: local or gen3

database:
  sqlite:
    file: "drs.db"
  # postgres:
  #   host: "localhost"
  #   port: 5432
  #   user: "drs"
  #   password: "secret"
  #   database: "drs"
  #   sslmode: "disable"

s3_credentials:
  - bucket: "my-bucket"
    region: "us-east-1"
    access_key: "ACCESS_KEY"
    secret_key: "SECRET_KEY"
    endpoint: "s3.amazonaws.com" # optional; set for MinIO/custom S3 endpoints
```

## Auth Modes

`auth.mode` is required and must be one of:

- `local`: local development mode. Use with SQLite.
- `gen3`: deployed authorization mode. Requires PostgreSQL unless mock auth is enabled.

## Database Requirements

- Local development should use SQLite.
- Gen3 deployment mode should use PostgreSQL.
- Local Gen3 behavior can be tested with mock auth (`DRS_AUTH_MOCK_ENABLED=true`) without Fence/Arborist.

## S3 Credentials

Each `s3_credentials` entry maps one bucket to credentials and region.

Fields:

- `bucket`: bucket name used for object registration and lookup.
- `region`: cloud region.
- `access_key`: credential access key.
- `secret_key`: credential secret.
- `endpoint` (optional): custom endpoint for MinIO or other S3-compatible backends.

## Local Example

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

## Mock Gen3 Environment Variables

For local integration testing of Gen3 auth flows, the following environment variables are supported:

- `DRS_AUTH_MODE=gen3`
- `DRS_AUTH_MOCK_ENABLED=true`
- `DRS_AUTH_MOCK_RESOURCES` (default: `/data_file`)
- `DRS_AUTH_MOCK_METHODS` (default: `*`)
- `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER=true` (optional)
