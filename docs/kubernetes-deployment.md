# Kubernetes Deployment

This page documents the current [`helm/syfon`](https://github.com/calypr/gen3-helm/tree/ohsu-develop/helm/syfon) chart behavior and the values that matter most for a real deployment.

For raw server config fields, see [Server Configuration](configuration.md). For local non-Kubernetes usage, see [Local Deployment](local-deployment.md).

## What The Chart Does

The current chart deploys Syfon by:

- rendering `.Values.config` into a Kubernetes Secret
- mounting that Secret at `/etc/drs/config.yaml`
- starting the container with `serve --config /etc/drs/config.yaml`
- injecting `DRS_DB_*` environment variables from a DB secret
- optionally running a PostgreSQL init job that creates the app role, creates the app database, and applies the Syfon schema

Important chart behavior:

- in `gen3` mode, if `config.auth.fence_url` is empty, the chart fills it from `https://<global.hostname>/user`
- the chart prefers `config.buckets`
- if you still set `config.s3_credentials`, the chart normalizes that into `buckets` in the rendered config
- the rendered Syfon config is stored as a Secret because it may contain bucket credentials and encryption material

## Recommended K8s Shape

For a normal Kubernetes deployment:

- use `config.auth.mode: gen3`
- use PostgreSQL
- provide `credential_encryption.master_key`
- configure bucket routing in `config.buckets`
- leave the DB connection wiring to the chart's `postgres.*` secrets unless you are reusing existing secrets

## Example Values

```yaml
image:
  repository: quay.io/ohsu-comp-bio/syfon
  tag: latest

config:
  port: 8080
  auth:
    mode: gen3
    # Optional. If omitted, the chart uses https://<global.hostname>/user
    fence_url: ""
  routes:
    docs: true
    ga4gh: true
    internal: true
    lfs: true
    metrics: true
  credential_encryption:
    master_key: REDACTED
  buckets:
    - bucket: cbds
      provider: s3
      region: us-east-1
      endpoint: https://object.example.org
      access_key: REDACTED
      secret_key: REDACTED
      resources:
        - organization: cbds
          org_path: organizations/cbds
          projects:
            - project_id: training
              project_path: projects/training

postgres:
  app:
    db_username: syfon_user
    db_password: REDACTED
    db_database: syfon_db
    db_sslmode: disable
  admin:
    username: postgres
    password: REDACTED
    database: postgres
  initJob:
    enabled: true
```

## PostgreSQL Wiring

The chart injects these into the Syfon container from the app DB secret:

- `DRS_DB_HOST`
- `DRS_DB_PORT`
- `DRS_DB_USER`
- `DRS_DB_PASSWORD`
- `DRS_DB_DATABASE`
- `DRS_DB_SSLMODE`

If you do not provide explicit app/admin DB hosts, the chart resolves them from `global.postgres.master.*` when present, or falls back to the release-local PostgreSQL service naming pattern.

## Reusing Existing Secrets

If your cluster already has DB secrets, set:

- `postgres.app.existingSecret`
- `postgres.admin.existingSecret` when `postgres.initJob.enabled=true`

The deployment still expects the app secret to expose:

- `db_host`
- `db_port`
- `db_username`
- `db_password`
- `db_database`
- `db_sslmode`

## Init Job

The init job is enabled by default.

What it does:

- waits for PostgreSQL readiness
- creates or updates the app role
- creates the app database if it does not exist
- grants database privileges
- applies the bundled Syfon PostgreSQL schema

If your database is already provisioned and schema-managed elsewhere, you can disable it with:

```yaml
postgres:
  initJob:
    enabled: false
```

## Health Probes

The chart configures both readiness and liveness probes against `GET /healthz` on the container `http` port. Tune them under:

- `probes.liveness.*`
- `probes.readiness.*`

## Install

```bash
helm upgrade --install syfon ./helm/syfon
```
