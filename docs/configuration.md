# Configuration

This document describes the `syfon` configuration model used by `serve --config <file>`.

## Configuration file

`syfon` accepts YAML or JSON config. Core sections:

- `port`
- `auth`
- `database`
- `s3_credentials`
- `lfs`
- `signing`

Example:

```yaml
port: 8080
auth:
  mode: local
  basic:
    username: "drs-user"
    password: "drs-pass"
database:
  sqlite:
    file: "drs.db"
s3_credentials:
  - bucket: "cbds"
    provider: "s3"
    region: "us-east-1"
    access_key: "cbds-user"
    secret_key: "<secret>"
    endpoint: "https://aced-storage.ohsu.edu/"
lfs:
  max_batch_objects: 1000
  max_batch_body_bytes: 10485760
  request_limit_per_minute: 1200
  bandwidth_limit_bytes_per_minute: 0
signing:
  default_expiry_seconds: 900
```

## Auth modes

### `auth.mode: local`

- Intended for local/dev flows.
- Works with SQLite.
- Optional HTTP basic auth:
  - `auth.basic.username`
  - `auth.basic.password`

### `auth.mode: gen3`

- Intended for deployed Gen3-integrated environments.
- Requires PostgreSQL unless mock auth is enabled (`DRS_AUTH_MOCK_ENABLED=true`).

Validation rules:

- `auth.mode` is required and must be `local` or `gen3`.
- If one of `auth.basic.username/password` is set, both must be set.

## Database

Exactly one database backend must be configured:

- `database.sqlite.file`
- or `database.postgres.{host,port,user,password,database,sslmode}`

If both are set, config load fails.

## `s3_credentials` providers

Supported providers in config:

- `s3` (default when omitted)
- `gcs` or `gs` (normalized to `gcs`)
- `azure` or `azblob` (normalized to `azure`)

For `provider: s3`, required fields are:

- `bucket`
- `region`
- `access_key`
- `secret_key`
- `billing_log_bucket`
- `billing_log_prefix`

`endpoint` is optional and commonly used for S3-compatible storage.

For `provider: gcs` and `provider: azure`, cloud billing log source fields are also required:

- `billing_log_bucket`
- `billing_log_prefix`

The same credential used for the data bucket must be able to list and read the billing log bucket/prefix. Syfon validates that log source when the bucket credential is added. Local `file` provider credentials are exempt from this cloud log requirement.

Transfer metrics do not block dashboard responses when a sync window is missing. Instead, metrics responses include freshness metadata showing whether the requested provider/bucket/time range is covered and when the latest completed sync ran.

Bucket validation follows provider rules:

- `s3`: DNS-style bucket names with lowercase letters, numbers, and hyphens.
- `gcs`: lowercase letters, numbers, hyphens, underscores, and dots; dotted names are allowed if each dot-separated segment is 1-63 characters.
- `azure`: lowercase letters, numbers, and hyphens only; consecutive hyphens are rejected.

## Environment variable overrides

Environment variables override config file values.

### Server/auth

- `DRS_PORT`
- `DRS_AUTH_MODE`
- `DRS_BASIC_AUTH_USER`
- `DRS_BASIC_AUTH_PASSWORD`

### LFS limits

- `DRS_LFS_MAX_BATCH_OBJECTS`
- `DRS_LFS_MAX_BATCH_BODY_BYTES`
- `DRS_LFS_REQUEST_LIMIT_PER_MINUTE`
- `DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE`

### Signing

- `DRS_SIGNING_DEFAULT_EXPIRY_SECONDS`

### Credential encryption

- `DRS_CREDENTIAL_KEY_MANAGER` (optional: `local` or `aws-kms`)
- `DRS_CREDENTIAL_KMS_KEY_ID` (when using AWS KMS)
- `DRS_CREDENTIAL_MASTER_KEY` (optional explicit local KEK override)
- `DRS_CREDENTIAL_LOCAL_KEY_FILE` (optional local key file path)

By default, Syfon uses `local` key management and auto-creates a server-side local KEK file. If `DRS_DB_SQLITE_FILE` is set, the local KEK defaults to the same directory (`.syfon-credential-kek`).

The local key file path can also be set in the Syfon config:

```yaml
credential_encryption:
  local_key_file: ".syfon-credential-kek"
```

If `DRS_CREDENTIAL_KMS_KEY_ID` is set (or `DRS_CREDENTIAL_KEY_MANAGER=aws-kms`), Syfon uses AWS KMS to wrap/unwrap per-record DEKs.

### SQLite/Postgres

- `DRS_DB_SQLITE_FILE`
- `DRS_DB_HOST`
- `DRS_DB_PORT`
- `DRS_DB_USER`
- `DRS_DB_PASSWORD`
- `DRS_DB_DATABASE`
- `DRS_DB_SSLMODE`

### Gen3 mock auth toggles

- `DRS_AUTH_MOCK_ENABLED`
- `DRS_AUTH_MOCK_RESOURCES`
- `DRS_AUTH_MOCK_METHODS`
- `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER`

### Authz cache tuning

- `DRS_AUTH_CACHE_ENABLED`
- `DRS_AUTH_CACHE_TTL_SECONDS`
- `DRS_AUTH_CACHE_NEGATIVE_TTL_SECONDS`
- `DRS_AUTH_CACHE_MAX_ENTRIES`
- `DRS_AUTH_CACHE_CLEANUP_SECONDS`

## Auth plugin setup

### LocalAuth (local mode)

- Set `auth.mode: local` in your config file.
- Optionally set HTTP basic auth credentials:
  - `auth.basic.username`
  - `auth.basic.password`
- Or use environment variables:
  - `DRS_BASIC_AUTH_USER`
  - `DRS_BASIC_AUTH_PASSWORD`
- **To use the plugin-based local auth:**
  - Build the plugin:
    ```sh
    make build-local-auth-plugin
    ```
  - Set:
    ```sh
    export SYFON_AUTHN_PLUGIN_PATH=bin/local_auth_plugin
    syfon serve --config config.yaml
    ```
- No external dependencies required for local mode.

### Gen3Auth (gen3 mode)

- Set `auth.mode: gen3` in your config file.
- Requires PostgreSQL unless mock auth is enabled.
- For local testing, enable mock auth with environment variables:
  - `DRS_AUTH_MOCK_ENABLED=true`
  - `DRS_AUTH_MOCK_RESOURCES="/data_file,/programs/my-org/projects/my-project"`
  - `DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete"`
  - `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER=true` (optional)
- In production, set the trusted Fence instance URL with `DRS_FENCE_URL` (must be a single valid `https://` URL, not a CSV list).
  - Example: `export DRS_FENCE_URL="https://fence.example.com"`
  - The server will only accept JWTs with an `iss` claim matching this URL, and will fetch JWKS from this endpoint.
- **To use the plugin-based Gen3 auth:**
  - Build the plugin:
    ```sh
    make build-gen3-auth-plugin
    ```
  - Set:
    ```sh
    export SYFON_AUTHN_PLUGIN_PATH=bin/gen3_auth_plugin
    syfon serve --config config.yaml
    ```

See README.md and deployment docs for more details.

## CLI usage

Run server:

```bash
syfon serve --config config_sample.yaml
```

Health check:

```bash
curl -s http://localhost:8080/healthz
```

## Local mode + git-drs tests

For `git-drs` local e2e tests, use:

- `auth.mode: local`
- optional basic auth configured in `auth.basic` or env
- valid `s3_credentials` entry for the bucket used by test scripts (`TEST_BUCKET`)

If tests fail with `bucket credential not found`, ensure bucket credentials exist for that bucket or enable test-side bootstrap (`TEST_CREATE_BUCKET_BEFORE_TEST=true` with bucket envs).
