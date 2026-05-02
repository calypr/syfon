# Troubleshooting

## Upload Failures

### `connection refused` on presigned PUT URL

**Symptom:**

```
upload request failed: Put "http://rgw.example.com/bucket/...": dial tcp x.x.x.x:80: connect: connection refused
```

**Cause:** The `endpoint` in `config.yaml` is using `http://` but the server only accepts HTTPS.

**Fix:** Update the endpoint to use `https://`:

```yaml
s3_credentials:
  - bucket: "my-bucket"
    endpoint: "https://rgw.example.com"  # not http://
```

Restart the server after changing the config for it to take effect.

---

### Upload prints both `successful uploaded` and `requested DID`

**Symptom:** The CLI upload output shows two IDs, for example:

```text
successfully uploaded 4c2f...
requested DID: e3b0...
```

**Meaning:** These are intentionally different concepts:

- `requested DID` is the ID the CLI asked the server to register.
- `successfully uploaded <id>` is the canonical object ID that Syfon stored.

When `--did` is omitted, the CLI deterministically mints the requested DID from
the file SHA256 plus the canonical project scope path
(`/organization/<org>/project/<project>`). The server uses the same rule when
it needs to mint an ID from checksum+scope data.

If no explicit `--did` is provided, `--project` must be provided. Organization-
only scope is not enough to mint a deterministic object ID.

**Rule of thumb:** Use the canonical ID from `successfully uploaded ...` for follow-up operations like `download`, `sha256sum`, and `rm`. Keep `requested DID` as the trace of what the client originally asked for.

---

### `bucket credential not found`

**Symptom:**

```
bucket credential not found
```

**Cause:** The bucket name in the upload request does not match any entry in `s3_credentials`.

**Fix:** Ensure the `bucket` field in your config exactly matches the bucket being targeted. If running integration tests, set `TEST_CREATE_BUCKET_BEFORE_TEST=true` along with the required bucket environment variables.

---

### Upload succeeds on server but client reports failure

**Symptom:** Server logs show `[201] POST /data/upload` but the client errors immediately after.

**Cause:** The presigned URL was generated successfully, but the subsequent `PUT` to the storage backend failed. This is a client-to-storage failure, not a client-to-Syfon failure.

**Check:**
- Can you reach the storage endpoint directly? (`mc alias set` + `mc ls`)
- Is the endpoint URL scheme correct (`http` vs `https`)?
- Is the storage backend reachable from the client's network?

---

## Server Startup Failures

### Server does not start

- Verify Go and dependencies are installed: `go version`
- Run tests to catch config or compilation issues: `make test-unit`
- Check for port conflicts: `lsof -i :8080`

---

### `no bucket credentials configured for upload`

**Cause:** `s3_credentials` is empty or missing in your config file.

**Fix:** Add at least one entry under `s3_credentials` in your config.

---

## Auth Issues

### All requests return `403 Forbidden` in `gen3` mode

- Confirm Fence and Arborist are reachable from the Syfon pod.
- For local testing, use mock auth to bypass Fence/Arborist:

```bash
DRS_AUTH_MOCK_ENABLED=true \
DRS_AUTH_MOCK_RESOURCES="/data_file" \
DRS_AUTH_MOCK_METHODS="read,file_upload,create,update,delete" \
go run . serve --config config.local.yaml
```

### `401 Unauthorized` instead of `403`

In `gen3` mode, Syfon returns `401` when no `Authorization` header is present. Set `DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER=false` (or omit the variable) to allow unauthenticated mock requests locally.

---

## Database Issues

### SQLite: `database is locked`

**Cause:** Multiple server processes are writing to the same SQLite file simultaneously.

**Fix:** Ensure only one server instance is running:

```bash
ps aux | grep syfon
```

SQLite is not suitable for multi-instance deployments — use PostgreSQL instead.

---

### PostgreSQL: `connection refused` or `password authentication failed`

- Verify `DRS_DB_HOST`, `DRS_DB_PORT`, `DRS_DB_USER`, `DRS_DB_PASSWORD`, and `DRS_DB_DATABASE` are set correctly.
- Check that the PostgreSQL schema has been initialized (via the Helm init Job or `db/scripts/`).
- Confirm `sslmode` matches your PostgreSQL server config (`disable`, `require`, `verify-full`, etc.).

---

## `git stash -u` hangs

**Cause:** `git stash -u` reads and compresses all untracked files, including any large test files (e.g. `test-file-10gb`).

**Fix:** Add large or generated files to `.gitignore` so they are excluded from stash operations:

```bash
echo "test-file-*" >> .gitignore
echo "drs_*.db" >> .gitignore
echo "cmd/**/__debug_bin*" >> .gitignore
```

---

## Documentation

### `make docs-serve` shows outdated content

MkDocs serves from source files directly in watch mode. If content appears stale, hard-refresh your browser (`Cmd+Shift+R` / `Ctrl+Shift+R`).

### Pages missing from the nav

Ensure all pages referenced under `nav:` in `mkdocs.yml` exist under `docs/`. Paths in `mkdocs.yml` are relative to the `docs/` directory (e.g. `configuration.md`, not `docs/configuration.md`).
