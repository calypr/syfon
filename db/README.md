# Database Architecture

This directory contains all persistence code for `drs-server`.

## Layout

- `core/`
  - shared interfaces, models, authz/resource helpers, typed DB errors
- `sqlite/`
  - SQLite implementation of `core.DatabaseInterface`
- `postgres/`
  - PostgreSQL implementation of `core.DatabaseInterface`
- `testing.go`
  - in-memory DB helper for tests

## Primary Tables

The server stores object metadata in a normalized schema:

### `drs_object`

Core record row (one row per object).

- `id` (PK, text): canonical object id (current implementation uses sha256 in many flows)
- `size` (bigint/int)
- `created_time` (timestamp)
- `updated_time` (timestamp)
- `name` (text)
- `version` (text)
- `description` (text)

### `drs_object_access_method`

One-to-many access locations for each object.

- `object_id` (FK -> `drs_object.id`)
- `url` (text), e.g. `s3://bucket/key`
- `type` (text), e.g. `s3`

### `drs_object_checksum`

One-to-many checksum values for each object.

- `object_id` (FK -> `drs_object.id`)
- `type` (text), e.g. `sha256`, `md5`
- `checksum` (text)

### `drs_object_authz`

One-to-many resource scopes used for RBAC checks.

- `object_id` (FK -> `drs_object.id`)
- `resource` (text), typically Arborist-style paths:
  - `/programs/<organization>/projects/<project>`
  - `/programs/<organization>`

### `s3_credential`

Bucket-level signing credentials.

- `bucket` (PK, text)
- `region` (text)
- `access_key` (text)
- `secret_key` (text)
- `endpoint` (text, optional)

## RBAC Model

- Local mode:
  - no gen3 RBAC enforcement
  - optional basic auth at middleware level
  - recommended local development database: SQLite
- Gen3 mode:
  - request middleware fetches privileges from Fence/Arborist context
  - DB/API/service checks evaluate object `authorizations` against user privileges
  - method-aware checks use `read/create/update/delete/file_upload`

## Developer Notes

### Add a new query or operation

1. Add method to `core.DatabaseInterface` in `core/interface.go`.
2. Implement method in both:
   - `sqlite/sqlite.go`
   - `postgres/postgres.go`
3. Update `testutils/mocks.go` if unit tests rely on the new method.
4. Add tests in service or API package.

### Local schema behavior

- SQLite schema is initialized in `sqlite.initSchema()`.
- PostgreSQL schema is expected to exist (via Helm init job/templates in `helm/drs-server`).
- SQLite helper scripts are provided in `db/scripts/`:
  - `init_sqlite.sql`
  - `init_sqlite_db.sh`

### Resource path abstraction

Use helpers in `core/resource_scope.go`:

- `ResourcePathForScope(org, project)`
- `ParseResourcePath(path)`

This lets API/business layers work with `organization/project` while still storing Arborist-compatible paths.
