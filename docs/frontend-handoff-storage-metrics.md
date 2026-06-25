# Frontend Handoff: Syfon Path-Level Storage Metrics

This note describes the backend storage-metrics work that was just added in
`syfon` so the frontend can stop recursively walking `/index` and computing
directory totals client-side.

## What Was Added

Two new metrics endpoints now exist under the metrics namespace:

1. `GET /index/v1/metrics/storage/summary`
2. `GET /index/v1/metrics/storage/children`

These are project-scoped path metrics backed by Syfon’s existing
`drs_object_browse_index` path model plus `drs_object.size` and
`drs_object.updated_time`.

The important part: the backend is reusing the same normalized browse-path
semantics as the existing path browsing queries, not inventing a second tree
definition.

## Why Frontend Should Use This

Old behavior:

- page through all `/index` results for a project
- rebuild a directory tree in the client
- sum bytes and counts locally

New behavior:

- ask Syfon for one path summary
- ask Syfon for direct children of the current path
- render the result directly

This removes:

- full-project pagination just to render a folder row
- repeated client aggregation work
- large response payloads for simple summary views

## Endpoint 1: Storage Summary

Route:

`GET /index/v1/metrics/storage/summary`

Required query params:

- `organization`
- `project`

Optional query params:

- `path`

If `path` is omitted, the summary is for the project root.

Example:

```http
GET /index/v1/metrics/storage/summary?organization=cbds&project=end_to_end_test&path=data
```

Response shape:

```json
{
  "organization": "cbds",
  "project": "end_to_end_test",
  "path": "data",
  "file_count": 2,
  "record_count": 3,
  "total_bytes": 60,
  "direct_child_count": 2,
  "duplicate_path_count": 1,
  "latest_update_time": "2026-06-24T22:00:00Z"
}
```

Field meanings:

- `file_count`: distinct normalized file paths under the subtree
- `record_count`: total indexed records under the subtree
- `total_bytes`: sum of `drs_object.size` for all matching records
- `direct_child_count`: number of immediate children under the requested path
- `duplicate_path_count`: count of normalized file paths with more than one indexed record
- `latest_update_time`: max `drs_object.updated_time` seen in the subtree

## Endpoint 2: Storage Children

Route:

`GET /index/v1/metrics/storage/children`

Required query params:

- `organization`
- `project`

Optional query params:

- `path`
- `limit`
- `offset`
- `sort_by`
- `sort_order`

Valid `sort_by` values:

- `name`
- `bytes`
- `updated_time`
- `records`

Valid `sort_order` values:

- `asc`
- `desc`

Example:

```http
GET /index/v1/metrics/storage/children?organization=cbds&project=end_to_end_test&path=data&sort_by=bytes&sort_order=desc
```

Response shape:

```json
{
  "organization": "cbds",
  "project": "end_to_end_test",
  "path": "data",
  "items": [
    {
      "name": "nested",
      "path": "data/nested",
      "type": "directory",
      "file_count": 1,
      "record_count": 1,
      "total_bytes": 30,
      "latest_update_time": "2026-06-24T22:00:00Z"
    },
    {
      "name": "a.txt",
      "path": "data/a.txt",
      "type": "file",
      "file_count": 1,
      "record_count": 2,
      "total_bytes": 30,
      "latest_update_time": "2026-06-24T22:00:00Z"
    }
  ]
}
```

Field meanings:

- `name`: direct child name only
- `path`: normalized full child path
- `type`: `file` or `directory`
- `file_count`: distinct normalized file paths represented by that child
- `record_count`: total records represented by that child
- `total_bytes`: summed `drs_object.size`
- `latest_update_time`: max update time across that child subtree

## Path Semantics

These endpoints intentionally follow Syfon’s existing browse-index path logic.

Important rules:

- path normalization strips duplicate `/`
- backslashes are normalized to `/`
- empty path means root
- `.` and `..` are rejected
- path aggregation is driven by the existing browse index

So the frontend should treat returned `path` values as canonical and should not
try to re-normalize them differently.

## Duplicate Records

The backend does not pretend duplicate-path records are one file.

If multiple indexed objects resolve to the same normalized path:

- all contribute to `record_count`
- all contribute to `total_bytes`
- `file_count` still counts the distinct path once
- duplicate occupancy is surfaced through `duplicate_path_count`

Frontend implication:

- if `record_count > file_count`, there are duplicate indexed records inside that subtree
- if a child row has `record_count > file_count`, that child has duplicate path occupancy

## Auth / Scope Requirements

These routes are not global rollups.

They currently require a concrete project scope:

- `organization` must be set
- `project` must be set

Do not call them for org-wide rollups yet.

## Suggested Frontend Wiring

For a file explorer/tree view:

1. On entering a project root, call `/metrics/storage/summary` with no `path`.
2. Also call `/metrics/storage/children` with no `path`.
3. On clicking a directory, call both endpoints again with that directory `path`.
4. Use `summary.direct_child_count` for quick folder metadata if useful.
5. Use `children.items` to render rows without reconstructing the subtree client-side.

For a summary card:

- use `/metrics/storage/summary`
- render `file_count`, `record_count`, `total_bytes`, and optionally `duplicate_path_count`

## Recommended UI Interpretation

For a directory row:

- primary size: `total_bytes`
- secondary count: `file_count`
- warning state if `record_count > file_count`

For a file row:

- size: `total_bytes`
- warning if `record_count > 1`

## What This Does Not Do

This is not a precomputed materialized quota table.

It is still query-time aggregation, but it is much cheaper than loading every
project record into the frontend and aggregating there.

This also does not add:

- org-wide grouped rollups
- deletion/remediation workflows
- duplicate cleanup

## Backend Files Touched

Useful reference points if the other agent wants to inspect behavior:

- `apigen/openapi/metrics.openapi.yaml`
- `internal/api/metrics/storage_summary.go`
- `internal/storagemetrics/storage_metrics.go`
- `internal/db/sqlite/metrics.go`
- `internal/db/postgres/metrics.go`
- `client/services/metrics_service.go`

## Short Pasteable Summary

Use Syfon’s new endpoints instead of recursively traversing `/index`:

- `GET /index/v1/metrics/storage/summary?organization=ORG&project=PROJ&path=OPTIONAL`
- `GET /index/v1/metrics/storage/children?organization=ORG&project=PROJ&path=OPTIONAL&sort_by=name|bytes|updated_time|records&sort_order=asc|desc&limit=N&offset=N`

These routes reuse the same normalized browse-index path semantics as existing
path browsing. `total_bytes` is summed from `drs_object.size`. `record_count`
counts all matching indexed records. `file_count` counts distinct normalized
paths. Duplicate path occupancy shows up when `record_count > file_count`, and
the summary also exposes `duplicate_path_count`.
