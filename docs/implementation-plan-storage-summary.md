# Implementation Plan: Path-Level Storage Summary Metrics

## Goal

Add native Syfon metrics endpoints for storage summaries so clients can ask for:

- directory size under `{organization, project, path}`
- direct-child breakdown for breadcrumb/tree navigation
- project-level totals without traversing every `/index` page
- organization/project-scoped storage metrics with documented semantics

This plan replaces the current frontend workaround that recursively walks
`/index` and computes aggregates client-side.

## Why This Is Needed

Current metrics support in Syfon is real, but it is focused on:

- per-object file usage counts
- transfer attribution and provider-observed transfer events
- scoped usage summaries by object

It does **not** currently provide:

- path-level storage rollups
- directory summaries by prefix
- direct-child storage listings for a tree UI
- a metrics model that returns bytes grouped by canonical path

That gap forces the frontend to re-enumerate the object index and rebuild a
directory tree on every request, which is the wrong ownership boundary.

## Scope

This plan covers new metrics read APIs and the storage-side aggregation needed
to power them.

This plan does **not** include:

- bucket/object deletion workflows
- automatic duplicate-object remediation
- quota enforcement
- billing reconciliation changes beyond reusing existing file size/object scope data

## Required Product Behaviors

The resulting API should support these use cases efficiently:

1. Project summary card:
   - total files
   - total bytes
   - total distinct direct children at the current path

2. Breadcrumb/tree view:
   - list direct children under a path
   - show each child type (`file` or `directory`)
   - show aggregate bytes for each child
   - show file coverage count for each child
   - show latest update timestamp for each child

3. Organization/project summary views:
   - project totals without full `/index` traversal
   - optional grouped rollups for organization-wide reporting later

## Canonical Data Semantics

These must be fixed before implementation to avoid frontend/backend drift.

### Path identity

Use a single canonical path field for storage-tree aggregation.

Recommended rule:

- canonical path = normalized `file_name` when present
- fallback = normalized `name` only when `file_name` is empty
- if neither is a usable path, exclude from path metrics and surface in logs/diagnostics

Rationale:

- `file_name` is already the closest thing to path-like user-facing placement
- `name` is too ambiguous to be the primary tree key
- access URLs should not define logical browse paths

### Size semantics

Use the indexed object size already stored in Syfon metadata.

- `total_bytes` = sum of `drs_object.size` for objects included in the filtered scope
- do not call cloud storage to compute live bucket bytes for interactive reads

Rationale:

- consistent with existing Syfon object metadata
- cheap to query
- avoids coupling UI latency to provider APIs

### Object/version semantics

Count every indexed object that is currently visible in the scope/path query.

That means:

- if two distinct objects resolve to the same canonical path, both contribute
- duplicate-path objects should be visible as `record_count > 1`
- deletion/supersession semantics remain whatever the object index currently exposes

Rationale:

- this is a storage summary of Syfon’s indexed state, not an inferred “clean file tree”
- remediation decisions should happen after the API can prove duplicate path occupancy

## Proposed API

Add a new storage-summary family under the metrics namespace.

### 1. Path summary

`GET /index/v1/metrics/storage/summary`

Query params:

- `organization` required
- `project` required
- `path` optional, default root

Response shape:

```json
{
  "organization": "Ellrott_Lab",
  "project": "embedding_rotation",
  "path": "data",
  "file_count": 6,
  "record_count": 6,
  "total_bytes": 50497716224,
  "direct_child_count": 6,
  "duplicate_path_count": 3,
  "latest_update_time": "2026-05-27T22:00:00Z"
}
```

### 2. Path children

`GET /index/v1/metrics/storage/children`

Query params:

- `organization` required
- `project` required
- `path` optional, default root
- `limit` optional
- `offset` optional
- `sort_by` optional: `bytes`, `name`, `updated_time`, `records`
- `sort_order` optional: `asc`, `desc`

Response shape:

```json
{
  "organization": "Ellrott_Lab",
  "project": "embedding_rotation",
  "path": "data",
  "items": [
    {
      "name": "tcga_HM450.hdf5",
      "path": "data/tcga_HM450.hdf5",
      "type": "file",
      "file_count": 1,
      "record_count": 2,
      "total_bytes": 39943195852,
      "latest_update_time": "2026-05-27T22:00:00Z"
    },
    {
      "name": "nested",
      "path": "data/nested",
      "type": "directory",
      "file_count": 42,
      "record_count": 42,
      "total_bytes": 90123123,
      "latest_update_time": "2026-05-20T18:14:00Z"
    }
  ]
}
```

### 3. Optional later: grouped project rollup

`GET /index/v1/metrics/storage/projects`

This is useful later for explorer summary tables, but it is not necessary to
replace the FileSummary workaround. Keep it out of phase 1 unless it is nearly
free once the query layer exists.

## Backend Design

### New model types

Add new metrics models in `internal/models`, for example:

- `StoragePathSummary`
- `StoragePathChild`
- `StoragePathChildrenResponse`

Fields should include:

- scope: `organization`, `project`, `path`
- counts: `file_count`, `record_count`, `direct_child_count`, `duplicate_path_count`
- bytes: `total_bytes`
- timestamps: `latest_update_time`

### New DB interface

Extend `internal/db/interface.go` with a dedicated storage metrics contract.

Suggested interface:

```go
type StorageMetricsStore interface {
    GetStoragePathSummary(ctx context.Context, organization, project, path string) (models.StoragePathSummary, error)
    ListStoragePathChildren(ctx context.Context, organization, project, path string, limit, offset int, sortBy, sortOrder string) ([]models.StoragePathChild, error)
}
```

This should remain separate from `FileUsageScopedLister`.

Reason:

- file usage metrics answer “how often was this object used?”
- storage summary metrics answer “how much indexed data is under this logical path?”

Those are different dimensions and should not be conflated.

### Query strategy

Do the aggregation in SQL against indexed metadata, not by hydrating every
object through `ObjectManager`.

The query layer needs to:

1. resolve visible objects in `{organization, project}`
2. normalize canonical browse path
3. filter to the requested prefix
4. compute:
   - direct child name
   - whether the child is a file or directory
   - aggregate bytes
   - file count
   - record count
   - latest update time

### Postgres / SQLite parity

Implement in both:

- `internal/db/postgres/metrics.go`
- `internal/db/sqlite/metrics.go`

Keep one behavioral contract and verify with matching tests.

## Normalization Rules

Path normalization should be shared and explicit.

Recommended helper package behavior:

- trim whitespace
- strip leading `/`
- collapse repeated `/`
- treat empty path as root
- direct child of root:
  - `foo.txt` -> file child `foo.txt`
  - `dir/a.txt` -> directory child `dir`

If a record has canonical path equal to the requested path exactly, treat it as
a file child only when querying the parent directory. Do not let a path
summarize itself as its own descendant.

## Authorization

Use the same auth pattern as the existing metrics server:

- require auth under Gen3 mode
- allow scoped reads only for readable resources
- require exact `organization/project` scope for path storage endpoints in phase 1

Do **not** implement organization-wide aggregation across arbitrary readable
scopes in the first pass. The storage monitor page already works at exact
project scope, and that keeps the SQL simpler.

## Phased Rollout

### Phase 1: backend path summary for exact project scope

Deliver:

- OpenAPI additions for:
  - `GET /index/v1/metrics/storage/summary`
  - `GET /index/v1/metrics/storage/children`
- generated metrics bindings via `make gen`
- new model types
- DB interface additions
- Postgres + SQLite implementations
- metrics server handlers
- unit/integration tests

Success criterion:

- the frontend can replace recursive `/index` traversal for a single project/path

### Phase 2: frontend migration

Deliver:

- replace `FileSummary` recursive fetch logic with metrics calls
- preserve duplicate-path visibility with `record_count`
- keep `/index` only for drill-down features that need raw record identity

Success criterion:

- `FileSummary` no longer walks every record to compute path totals

### Phase 3: broader rollups and diagnostics

Optional additions:

- organization-wide grouped project totals
- duplicate-path diagnostic endpoint
- “top N duplicate paths” for cleanup/admin views

Success criterion:

- explorer/admin UIs can build storage summaries without re-aggregating client-side

## Testing Plan

Add tests at three layers.

### DB tests

For both Postgres and SQLite:

- root path summary
- nested path summary
- direct-child breakdown
- duplicate canonical path aggregation
- mixed file/directory children
- empty path result

### API tests

Under `internal/api/metrics`:

- auth required / forbidden scope
- bad query validation
- summary response shape
- children response shape
- pagination and sorting

### Server parity tests

Update endpoint registration tests to include the new routes.

## Risks

### Path ambiguity

If `file_name` is missing or inconsistent, tree metrics will be noisy.

Mitigation:

- define canonical path rules once
- add tests covering `file_name` vs `name`
- optionally add a diagnostic counter for records excluded from path metrics

### Duplicate-path confusion

Aggregating by path can hide the fact that multiple objects occupy one path.

Mitigation:

- return both `file_count` and `record_count`
- return `duplicate_path_count` in summaries

### SQL complexity drift

It is easy to accidentally reimplement the object manager in SQL.

Mitigation:

- keep phase 1 scoped to exact `{organization, project, path}`
- avoid organization-wide multi-scope fanout until the project-path query is stable

## Concrete Implementation Order

1. Add new OpenAPI paths and schemas to `apigen/openapi/metrics.openapi.yaml`.
2. Run `make gen`.
3. Add `StoragePathSummary` and `StoragePathChild` models.
4. Extend `internal/db/interface.go` with a storage metrics interface.
5. Implement SQLite queries first in `internal/db/sqlite/metrics.go`.
6. Mirror the behavior in `internal/db/postgres/metrics.go`.
7. Add metrics handlers in `internal/api/metrics`.
8. Add tests for handlers and DB parity.
9. Switch the frontend `FileSummary` page to use the new endpoints.

## Immediate Recommendation

Implement phase 1 only on this branch.

That is the smallest meaningful delivery because it:

- removes the frontend recursive `/index` workaround
- establishes the right metrics ownership boundary in Syfon
- preserves future room for duplicate-path diagnostics without coupling them to cleanup workflows
