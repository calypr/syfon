# Deployment

This page captures current deployment expectations without prescribing a single platform-specific rollout.

## Mode and Database Matrix

- `auth.mode: local`: intended for local development and testing; use SQLite.
- `auth.mode: gen3`: intended for deployed environments; use PostgreSQL.

Local Gen3 behavior can be tested with mock auth enabled, but that is not a production substitute.

## PostgreSQL Schema Initialization

For deployment environments using PostgreSQL, schema initialization is managed by the Helm chart:

- `helm/drs-server/templates/postgres-schema-configmap.yaml`
- related init Job resources in the chart

This repository intentionally does not provide a separate standalone PostgreSQL init SQL script for deployments.

## Container and Runtime

- A `Dockerfile` is provided for containerized builds.
- Server process can be launched with `go run . serve --config <config.yaml>` or with `make serve ARGS="--config <config.yaml>"`.

## Recommended Pre-Deploy Checks

1. `go test ./... -count=1`
2. Validate config includes required `auth.mode` and database settings for the target mode.
3. Confirm S3 credentials entries cover all required buckets.
