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
- `--server`: target Syfon URL (defaults to `SYFON_SERVER_URL`, then `DRS_SERVER_URL`, then `http://127.0.0.1:8080`)
- `--batch-size`: records per batch (default `100`)
- `--limit`: max records to migrate (`0` means all)
- `--dry-run`: do not write to Syfon
- `--default-authz`: comma-separated default authz scopes for records with empty authz
- `--version`: print version

## Example commands

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

