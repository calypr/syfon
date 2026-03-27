# Development

## Common Commands

- `make gen`: generate DRS server code from the GA4GH OpenAPI spec and overlays.
- `make test`: run all unit and integration tests.
- `make test-unit`: run unit tests only.
- `make coverage`: run core package coverage and write reports under `coverage/`.
- `make coverage-full`: run broader compatibility-layer coverage.
- `make serve ARGS="--config config.local.yaml"`: start the server via `cmd/server`.
- `make docs`: serve MkDocs locally at `http://localhost:8000`.

## OpenAPI and Submodule Workflow

The GA4GH schema submodule is located at:

- `ga4gh/data-repository-service-schemas`

Initialize/update it with:

```bash
make init-schemas
# or
git submodule update --init --recursive
```

Generate with the default spec path:

```bash
make gen
```

Override the source spec when needed:

```bash
make gen OPENAPI=/path/to/custom/openapi.yaml
```

## Integration Tests

Run integration tests with a specific config file:

```bash
go test ./cmd/server -v -count=1 -testConfig=config.yaml
```

## Local Development Loop

```bash
go test ./... -count=1
./db/scripts/init_sqlite_db.sh drs_local.db
go run . serve --config config.local.yaml
```