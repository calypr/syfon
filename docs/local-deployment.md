# Local Deployment

Local Syfon deployments should normally use SQLite plus `auth.mode: local`. You do not need PostgreSQL for a local run.

If you only want the fastest first run, use [Quick Start](quickstart.md). If you need the raw config field reference, use [Server Configuration](configuration.md).

## Recommended Local Shape

For local development:

- use `database.sqlite`
- use `auth.mode: local`
- use `auth.basic.*`
- point bucket credentials at a local or dev S3-compatible endpoint such as MinIO

## Example Local Config

```yaml
port: 8080

auth:
  mode: local
  basic:
    username: drs-user
    password: drs-pass

database:
  sqlite:
    file: ./data/drs_local.db

credential_encryption:
  local_key_file: ./data/.syfon-credential-kek

buckets:
  - bucket: local-bucket
    provider: s3
    region: us-east-1
    endpoint: http://localhost:9000
    access_key: minio-user
    secret_key: minio-pass
```

Why this shape works:

- SQLite keeps the metadata store self-contained
- `auth.mode: local` avoids the Gen3, Fence, and PostgreSQL requirements
- `local_key_file` gives the local credential KEK an explicit stable path next to the SQLite DB

## Run From Source

```bash
bin/syfon serve --config config.local.yaml
```

## Run With Docker

```bash
mkdir -p data
docker run \
  -p 8080:8080 \
  --user "$(id -u):$(id -g)" \
  --workdir / \
  -v "$(pwd)/config.local.yaml:/config.yaml:ro" \
  -v "$(pwd)/data:/data" \
  quay.io/ohsu-comp-bio/syfon:development serve --config /config.yaml
```

The working directory makes the config's `./data` paths resolve inside the mounted `/data` directory. Running as your host user lets Syfon write the SQLite database and local encryption key there.

After the server has started, stop it and run this check. Run it again after starting and stopping a replacement container with the command above. The database inode and key hash must match. SQLite can update the database file during startup, so its file hash can change.

```bash
docker run --rm \
  --entrypoint sh \
  -v "$(pwd)/data:/data:ro" \
  quay.io/ohsu-comp-bio/syfon:development \
  -c 'test -s /data/drs_local.db && test -s /data/.syfon-credential-kek && stat -c %i /data/drs_local.db && sha256sum /data/.syfon-credential-kek'
```

Use the published image from [Quay](https://quay.io/repository/ohsu-comp-bio/syfon?tab=tags) when you do not want to build locally.

## Smoke Test

```bash
curl -u drs-user:drs-pass http://localhost:8080/healthz
```

## Local Auth

The documented local path is `auth.basic.username` plus `auth.basic.password`. Set `auth.allow_unauthenticated: true` only for development or tests that need to omit credentials.

## Notes

- `auth.mode: gen3` is not the normal local path because it requires PostgreSQL unless you are explicitly using mock auth for integration testing.
- If your bucket credentials are non-empty, Syfon still needs valid credential encryption at runtime. `credential_encryption.local_key_file` is the simplest local option.
