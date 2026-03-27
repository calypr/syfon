# Build

This guide covers building the `drs-server` binary and regenerating API code.

## Prerequisites

- Go 1.26+
- Git
- Docker (required for OpenAPI generation)

## 1. Clone and enter the repository

```bash
git clone <your-repo-url>
cd drs-server
```

## 2. Initialize GA4GH schemas submodule

```bash
make init-schemas
# or
git submodule update --init --recursive
```

## 3. Regenerate OpenAPI-derived code

```bash
make gen
```

Use a custom source spec when needed:

```bash
make gen OPENAPI=/path/to/custom/openapi.yaml
```

## 4. Build the server

```bash
make build
# or
go build ./...
```

## 5. Run tests

```bash
make test
# or
go test ./... -count=1
```

## Build-Adjacent Commands

- `make test-unit`: Unit tests only.
- `make coverage`: Core package coverage reports.
- `make coverage-full`: Broader compatibility-layer coverage.
