# Deployment

## Local Development

### Prerequisites

- Go 1.24+
- SQLite3 (`sqlite3`)

### Run the server

```bash
go run . serve --config config.local.yaml
```

Or via Make:

```bash
make serve
```

### Build the docs

```bash
make docs
```

### Serve the docs locally

```bash
make docs-serve
```

---

## Docker

A `Dockerfile` is included in the repository root.

### Build the image

```bash
docker build -t syfon:latest .
```

### Run the container

Mount your config file and (for SQLite) a data volume:

```bash
docker run \
  -p 8080:8080 \
  -v $(pwd)/config.local.yaml:/config.yaml \
  -v $(pwd)/data:/data \
  syfon:latest serve --config /config.yaml
```

### Pre-built images

Pre-built multi-arch images (linux/amd64, linux/arm64) are published to Quay on every push to `main`:

```
quay.io/ohsu-comp-bio/syfon:latest
quay.io/ohsu-comp-bio/syfon:<branch>
quay.io/ohsu-comp-bio/syfon:<git-sha>
```

---

## Kubernetes

### Configuration

Store your Syfon config as a `ConfigMap` and any sensitive values (S3 credentials, DB password) as `Secret` resources. Mount them into the pod and reference via environment variables or `--config`.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: syfon-config
data:
  config.yaml: |
    port: 8080
    auth:
      mode: gen3
    database:
      postgres:
        host: postgres-svc
        port: 5432
        user: syfon
        database: syfon
        sslmode: require
    s3_credentials:
      - bucket: "my-bucket"
        region: "us-east-1"
        access_key: ""   # set via DRS_CREDENTIAL_* env or KMS
        secret_key: ""
        endpoint: "https://rgw.example.com"
```

### PostgreSQL schema initialization

The server bootstraps the object schema on startup, including the tables and foreign keys needed for cascade delete behavior.
The Helm chart still provides an init job for deployments that prefer pre-created schema, but it is no longer required for a fresh Postgres database.

### Health and readiness probes

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## Auth Modes

### `local` (development)

- Works with SQLite.
- Optional HTTP basic auth via `auth.basic.username/password` or `DRS_BASIC_AUTH_USER` / `DRS_BASIC_AUTH_PASSWORD`.
- Optional CSV-backed local authz via `auth.local_authz_csv` or `DRS_LOCAL_AUTHZ_CSV`.
  If no CSV is provided, local Basic Auth remains admin/unrestricted.
- No external auth service required.

### `gen3` (production)

- Requires PostgreSQL.
- Integrates with Fence (identity) and Arborist (policy).
- For local integration testing without Fence/Arborist, use mock auth:

```bash
DRS_AUTH_MODE=gen3 \
DRS_AUTH_MOCK_ENABLED=true \
DRS_AUTH_MOCK_RESOURCES="/data_file,/programs/my-org/projects/my-project" \
DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete" \
go run . serve --config config.local.yaml
```

---

## Credential Encryption

By default, Syfon encrypts S3 `access_key` and `secret_key` at rest using a local KEK file.

In production, use AWS KMS:

```bash
DRS_CREDENTIAL_KEY_MANAGER=aws-kms
DRS_CREDENTIAL_KMS_KEY_ID=arn:aws:kms:us-east-1:123456789:key/...
```

See [Encryption](encryption.md) for full details.

---

## Installing the CLI

A prebuilt binary for your platform can be installed via the install script:

```bash
curl -fsSL https://raw.githubusercontent.com/calypr/syfon/main/install.sh | bash
```

Or build from source:

```bash
make install
```

Check the installed version:

```bash
syfon version
```
