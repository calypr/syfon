# ADR: drs_object Read/Write Endpoint Surface

- Status: Accepted
- Date: 2026-04-13
- Decision Owners: Syfon maintainers

## Context

Operational debugging and migration readiness required a clear inventory of which API endpoints read from or write to the `drs_object` table (directly or through database interface methods that join/update object metadata).

Without a single source of truth, it is easy to miss side-effecting routes (for example, upload and LFS verification flows that create or register objects) and to underestimate read dependencies used for authorization, lookup, and metrics.

## Decision

We maintain an ADR-backed endpoint inventory for `drs_object` access, grouped by behavior:

1. Read-only endpoints.
2. Write-only endpoints.
3. Read-then-write endpoints.

The inventory below is the current baseline.

## Read and Write `drs_object`

- `POST /ga4gh/drs/v1/objects/{object_id}/delete`
- `POST /ga4gh/drs/v1/objects/delete`
- `POST /index`
- `PUT /index/{id}`
- `DELETE /index`
- `POST /index/bulk/delete`
- `POST /data/upload` (creates blank record when not found)
- `POST /data/multipart/init` (creates blank record when not found)
- `POST /info/lfs/verify` (registers object from staged metadata when not found)

## Write `drs_object` Only

- `POST /ga4gh/drs/v1/objects/register`
- `POST /index/bulk`
- `POST /index/migrate/bulk`
- `DELETE /index/{id}`

## Read `drs_object` Only

- `GET /ga4gh/drs/v1/objects/{object_id}`
- `POST /ga4gh/drs/v1/objects/{object_id}`
- `OPTIONS /ga4gh/drs/v1/objects/{object_id}`
- `POST /ga4gh/drs/v1/objects`
- `GET /ga4gh/drs/v1/objects/{object_id}/access/{access_id}`
- `POST /ga4gh/drs/v1/objects/{object_id}/access/{access_id}`
- `GET /ga4gh/drs/v1/objects/checksum/{checksum}`
- `POST /ga4gh/drs/v1/objects/{object_id}/access-methods`
- `POST /ga4gh/drs/v1/objects/access-methods`
- `GET /index`
- `GET /index/{id}`
- `POST /index/bulk/hashes`
- `POST /index/bulk/sha256/validity`
- `POST /index/bulk/documents`
- `GET /data/download/{file_id}`
- `GET /data/download/{file_id}/part`
- `GET /data/upload/{file_id}`
- `POST /data/upload/bulk`
- `POST /data/multipart/upload`
- `POST /data/multipart/complete`
- `POST /info/lfs/objects/batch`
- `PUT /info/lfs/objects/{oid}`
- `GET /index/v1/metrics/files`
- `GET /index/v1/metrics/files/{object_id}`
- `GET /index/v1/metrics/summary`
- `POST /index/v1/sha256/validity`

## Consequences

- Operators and developers have a single place to audit object metadata blast radius.
- Schema-change planning for `drs_object` can now include endpoint impact review.
- Security and authorization reviews can focus on this route set for read/write permissions.

## Follow-up

- Keep this inventory updated when adding or changing handlers that call:
  - `GetObject`, `GetBulkObjects`, `GetObjectsByChecksum`, `GetObjectsByChecksums`,
  - `CreateObject`, `RegisterObjects`, `DeleteObject`, `BulkDeleteObjects`.

