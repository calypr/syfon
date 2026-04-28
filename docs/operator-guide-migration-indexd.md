# Operator Guide: Indexd to Syfon Migration

`syfon migrate` is a two-step offline workflow:

1. `syfon migrate export` reads records from a source Gen3 Indexd-compatible `/index` API and writes a local SQLite dump.
2. `syfon migrate import` reads that dump and loads records into a target Syfon instance through Syfon's existing `/index/bulk` compatibility loader.

No migration-specific Syfon server endpoint is required.

## Export

```bash
syfon migrate export \
  --indexd-url "https://source-gen3.example.org" \
  --source-profile source \
  --dump ./indexd-records.sqlite \
  --batch-size 500
```

The exporter uses Indexd's DID cursor (`start=<last_did>`) rather than page offsets and de-duplicates records by DID. A single sweep is sufficient for a quiet Indexd database; increase `--sweeps` only if the source is being modified during export or the deployment is known to return inconsistent pages.

Useful export flags:

- `--limit 1000`: canary export with only the first N unique records.
- `--dry-run`: fetch, transform, and validate without writing the dump.
- `--default-authz /programs/open`: apply authz only when a source record has no `authz`.
- `--source-token`: use a raw bearer token instead of a Gen3 profile.

## Import

```bash
syfon migrate import \
  --dump ./indexd-records.sqlite \
  --server "https://target-gen3-with-syfon.example.org" \
  --target-profile target \
  --batch-size 500
```

The import command reads the SQLite dump in batches and posts to the existing `POST /index/bulk` endpoint.

## Validation

After a canary or full run, spot-check a few records:

```bash
curl -s "https://target-gen3-with-syfon.example.org/index/<DID>" | jq .
curl -s -X POST "https://target-gen3-with-syfon.example.org/index/bulk/hashes" \
  -H "Content-Type: application/json" \
  -d '{"hashes":["sha256:<SHA256>"]}' | jq .
```

The migration preserves DIDs, hashes, URLs, file names, descriptions, versions, timestamps, and authz-derived organization/project access scopes for normal Indexd object records. Deprecated Indexd fields such as `baseid`, `rev`, `acl`, `metadata`, `urls_metadata`, `form`, and `uploader` are intentionally not loaded into Syfon.
