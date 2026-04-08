# syfon-migrate

`syfon-migrate` is a dedicated migration binary for moving records from an Indexd-compatible source into Syfon.

It wraps the shared migration library in `github.com/calypr/syfon/migrate` and is intentionally separate from the main `syfon` CLI binary.

## Build

From repository root:

```bash
make build-migrate
```

Or directly:

```bash
cd apps/migrate
go build -o ../../bin/syfon-migrate .
```

## Test

From repository root:

```bash
make test-migrate
```

Or directly:

```bash
cd apps/migrate
go test ./...
```

## Usage

```bash
bin/syfon-migrate --indexd-url https://indexd.example.org --server http://127.0.0.1:8080
```

Flags:

- `--indexd-url` (required): source Indexd URL
- `--indexd-profile`: optional local Gen3 profile used for authenticated Indexd reads
- `--server`: target Syfon URL (defaults to `SYFON_SERVER_URL`, then `DRS_SERVER_URL`, then `http://127.0.0.1:8080`)
- `--batch-size`: records per batch (default `100`)
- `--limit`: max records to migrate (`0` means all)
- `--dry-run`: do not write to Syfon
- `--default-authz`: comma-separated default authz scopes for records with empty authz
- `--version`: print version

## Example commands

Don't forget to start server before running migration:
```aiignore
make serve ARGS="--config config.local.yaml --server http://127.0.0.1:8080"

```


Dry run:

```bash
bin/syfon-migrate \
  --indexd-url https://indexd.example.org \
  --server http://127.0.0.1:8080 \
  --dry-run
```

Canary run:

```bash
bin/syfon-migrate \
  --indexd-url https://indexd.example.org \
  --server http://127.0.0.1:8080 \
  --limit 1000 \
  --batch-size 200
```

With default authz fallback:

```bash
bin/syfon-migrate \
  --indexd-url https://indexd.example.org \
  --server http://127.0.0.1:8080 \
  --default-authz /programs/open,/programs/shared
```

With authenticated source reads via Gen3 profile:

```bash
bin/syfon-migrate \
  --indexd-url https://indexd.example.org \
  --profile default \
  --server http://127.0.0.1:8080 \
  --dry-run
```

This expects the profile to exist in `~/.gen3/gen3_client_config.ini`.


## Verify
```aiignore
$ bin/syfon-migrate --indexd-url https://calypr-dev.ohsu.edu/index --server http://127.0.0.1:8080 --profile calypr-dev --batch-size 1000

migration complete: fetched=82628 transformed=82628 loaded=82628 skipped=0 errors=0

```

The server should log each batch as it's processed, and the final stats line should show all records fetched, transformed, and loaded with zero errors.
```bash
time=2026-04-08T15:10:55.842-07:00 level=INFO msg="POST /index/migrate/bulk InternalMigrateBulk 101.907542ms"
time=2026-04-08T15:10:55.842-07:00 level=DEBUG msg="[200] POST /index/migrate/bulk" request_id=016f270a205456af3c9398a6 status=200 duration_ms=101
```


To verify migrated records in Syfon, you can use the `sqlite3` CLI :

```bash
$ sqlite3 drs_local.db

sqlite> select count(object_id) as object_id_count, resource from drs_object_authz group by resource order by object_id_count desc ;
41351|/programs/gdc_mirror/projects/gdc_mirror
30859|/programs/aced/projects/evotypes
...
sqlite> select count(object_id) from drs_object_authz;
81635

```
