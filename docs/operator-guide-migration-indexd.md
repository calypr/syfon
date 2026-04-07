# Operator Guide: Indexd to Syfon Migration

This guide explains how to configure, run, and validate the `syfon migrate` workflow introduced for issue #20.

## What this guide covers

- Source and target configuration
- Dry-run and live migration commands
- Post-run validation checks
- Safe rerun behavior and rollback options
- Common troubleshooting steps

## Migration behavior (quick reference)

`syfon migrate` runs an API-driven ETL pipeline:

1. Extract records from source `IndexdURL` (`GET /index`)
2. Transform Indexd fields to Syfon DRS model
3. Validate each transformed object
4. Load records into Syfon (`POST /index/migrate/bulk`, internal-only route)

It is idempotent. Re-running the same migration does not create duplicate records.

## 1) Prerequisites

- A running source Indexd-compatible API reachable at `<INDEXD_URL>`
- A running target Syfon server reachable at `<SYFON_URL>`
- Built `syfon` CLI binary (or `go run .` in this repo)
- Credentials/network access to read source and write target

Optional but recommended:

- A DB snapshot before live migration
  - SQLite: file copy backup
  - Postgres: `pg_dump`

## 2) Configuration

Set source and target endpoints:

```bash
export INDEXD_URL="https://indexd.example.org"
export SYFON_SERVER_URL="http://127.0.0.1:8080"
```

Notes:

- `--indexd-url` is required.
- Target defaults come from `--server`, then `SYFON_SERVER_URL`, then `DRS_SERVER_URL`, then `http://127.0.0.1:8080`.

Useful flags:

- `--batch-size` (default `100`)
- `--limit` (`0` means all records)
- `--dry-run` (no writes)
- `--default-authz` (applied only when source record has empty `authz`)

## 3) Run migration

### Step A: Dry run (required first)

```bash
./syfon migrate \
  --indexd-url "$INDEXD_URL" \
  --server "$SYFON_SERVER_URL" \
  --dry-run
```

Expected output ends with a stats line like:

- `fetched=... transformed=... loaded=... skipped=... errors=...`

In dry-run mode, `loaded` means "would load".

### Step B: Limited canary run

Run a small subset first:

```bash
./syfon migrate \
  --indexd-url "$INDEXD_URL" \
  --server "$SYFON_SERVER_URL" \
  --batch-size 100 \
  --limit 1000
```

### Step C: Full run

```bash
./syfon migrate \
  --indexd-url "$INDEXD_URL" \
  --server "$SYFON_SERVER_URL" \
  --batch-size 500
```

If you need a default authz for legacy records missing authz:

```bash
./syfon migrate \
  --indexd-url "$INDEXD_URL" \
  --server "$SYFON_SERVER_URL" \
  --default-authz /programs/open
```

## 4) Validate migration

## Health checks

```bash
curl -s "$SYFON_SERVER_URL/healthz"
```

## Spot-check migrated records

Pick sample DIDs from the source and verify target fields:

```bash
curl -s "$SYFON_SERVER_URL/index/<DID>" | jq .
```

Confirm:

- `did` matches source `did`
- `hashes` preserved
- `urls` preserved
- `authz` preserved
- deprecated fields are absent from migration input/output behavior

## Query checks by hash

```bash
curl -s -X POST "$SYFON_SERVER_URL/index/bulk/hashes" \
  -H "Content-Type: application/json" \
  -d '{"hashes":["sha256:<SHA256>"]}' | jq .
```

## Batch completeness sanity check

Compare dry-run and live run totals from command output:

- `fetched` should match expected source count for same scope/limit
- `errors` should be `0` (or investigated)
- `skipped` should be understood (usually records failing validation)

## 5) Rerun and rollback

## Rerun

Re-running the same command is safe and expected for retry/recovery.

```bash
./syfon migrate --indexd-url "$INDEXD_URL" --server "$SYFON_SERVER_URL"
```

## Rollback options

- Preferred: restore DB from pre-migration snapshot.
- If partial cleanup is needed, use targeted delete APIs for known scopes/hashes in controlled maintenance windows.

## 6) Troubleshooting

**`--indexd-url is required`**

- Provide `--indexd-url` explicitly.

**Source fetch errors (`fetch page ...`)**

- Verify source endpoint and connectivity.
- Test manually:

```bash
curl -s "$INDEXD_URL/index?limit=1" | jq .
```

**Load errors (`load batch ...`)**

- Verify target server URL and auth/network path.
- Verify target API responds:

```bash
curl -s "$SYFON_SERVER_URL/healthz"
```

**High `skipped` count**

- Skips usually indicate validation failures (for example, missing checksums).
- Use dry-run logs to identify affected IDs and repair source records.

**Unexpected authz gaps**

- Use `--default-authz` for records missing `authz` in the source.

## 7) Recommended production runbook

1. Take target DB snapshot
2. Run dry-run
3. Run canary (`--limit`)
4. Validate samples and hash lookups
5. Run full migration
6. Validate again and archive migration logs/stats

