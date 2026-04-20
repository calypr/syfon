# Operator Guide: OpenAPI Schema Loading, Code Generation, and Swagger Serving

This guide explains where Syfon loads OpenAPI schemas, how each code generation task works, how specs are served at Swagger endpoints, and how to update the GA4GH schema version.

## 1) Schema sources and default inputs

### GA4GH DRS canonical schema

Syfon vendors the upstream GA4GH DRS schemas as a Git submodule:

- Submodule path: `ga4gh/data-repository-service-schemas`
- Default DRS OpenAPI input: `ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml`

The `Makefile` defaults are:

```make
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
SCHEMAS_SUBMODULE ?= ga4gh/data-repository-service-schemas
```

### Additional local OpenAPI inputs

Syfon also maintains local OpenAPI inputs for additional generated packages:

- `apigen/api/lfs.openapi.yaml`
- `apigen/api/bucket.openapi.yaml`
- `apigen/api/metrics.openapi.yaml`
- `apigen/api/internal.openapi.yaml`

## 2) OpenAPI code generation tasks in this repository

The repository contains multiple generation tasks (not only DRS). All are implemented in the `Makefile`.

| Target | Input | Output package | Generator mode |
|---|---|---|---|
| `make gen` | `$(OPENAPI)` (GA4GH submodule spec) + overlay merge | `apigen/drs` + bundled `apigen/api/openapi.yaml` | `openapi-generator` `go-server` |
| `make gen-lfs` | `apigen/api/lfs.openapi.yaml` | `apigen/lfsapi` | `openapi-generator` `go` (models + utils) |
| `make gen-bucket` | `apigen/api/bucket.openapi.yaml` | `apigen/bucketapi` | `openapi-generator` `go` (models + utils) |
| `make gen-metrics` | `apigen/api/metrics.openapi.yaml` | `apigen/metricsapi` | `openapi-generator` `go` (models + utils) |
| `make gen-internal` | `apigen/api/internal.openapi.yaml` | `apigen/internalapi` | `openapi-generator` `go` (models + utils) |

### Notes on `make gen`

`make gen` performs this DRS flow:

1. Validate input and (optionally) init submodule.
2. Bundle upstream GA4GH DRS schema with Redocly into `.tmp/drs.base.yaml`.
3. Merge Syfon overlay (`apigen/specs/drs-extensions-overlay.yaml`) into `apigen/api/openapi.yaml` via `yq`.
4. Generate DRS server stubs and interface code (`go-server`) into `.tmp/apigen.gen/drs`.
5. Copy generated server package to `apigen/drs` and generator metadata files into `apigen/`.
6. Re-run the package-specific model generation targets when their specs are present so the shared `apigen/*` contracts stay in sync.

### How generated code is used

The generated surface is split deliberately:

- `apigen/drs` contains the generated DRS server interfaces, routers, models, and helper types. The handwritten server implementation in `cmd/server` wires those generated interfaces into the runtime router and middleware.
- `apigen/lfsapi`, `apigen/bucketapi`, `apigen/metricsapi`, and `apigen/internalapi` contain generated model packages used by the runtime code and client-side helpers.
- The `client` module is handwritten, but it consumes the generated `apigen` types so request/response shapes remain aligned with the OpenAPI contracts.
- Custom behavior that should not be regenerated lives behind the `.openapi-generator-ignore` files and the handwritten router/service code.

The practical result is that OpenAPI generation keeps both sides of the API in sync:

- server changes regenerate the DRS stub and model surface
- client-side request/response types track the same contract
- handwritten business logic stays stable while the schema evolves

### Common change recipes

Use this section when you are deciding what to edit and what command to run next.

| Change you are making | Edit these files first | Regenerate with | Why |
|---|---|---|---|
| Add or remove a DRS endpoint | `ga4gh/data-repository-service-schemas` submodule or `apigen/specs/drs-extensions-overlay.yaml` | `make gen` | Keeps the bundled DRS spec and generated server stub package aligned. |
| Add or change a DRS field or model | Upstream spec or overlay | `make gen` | Regenerates `apigen/drs` models and request/response shapes. |
| Change LFS/Bucket/Metrics/Internal request or response shapes | `apigen/api/*.openapi.yaml` | `make gen-lfs`, `make gen-bucket`, `make gen-metrics`, or `make gen-internal` | Refreshes the package that owns that contract. |
| Change route behavior, auth, middleware, or validation logic only | `cmd/server`, `internal/api/*`, `internal/api/middleware/*` | No regen needed | These are handwritten runtime concerns, not generated contracts. |
| Update docs served at `/index/openapi.yaml` | `apigen/api/*.openapi.yaml` and any asset/embed code | The relevant `make gen*` target plus tests | The runtime docs endpoint reads the bundled spec files. |

### Practical workflow

1. Make the schema or handler change.
2. Regenerate the affected package(s).
3. Review the diff in both the spec and generated code.
4. Run the impacted tests, not just `go test ./...` by habit.
5. Commit the generated files together with the source change so the branch stays reproducible.

### What usually changes in git

- DRS changes usually touch the submodule pointer, `apigen/api/openapi.yaml`, `apigen/drs/*`, and sometimes `docs/index.md` or `README.md`.
- LFS changes usually touch `apigen/api/lfs.openapi.yaml` and `apigen/lfsapi/*`.
- Bucket, metrics, and internal schema changes follow the same pattern in their corresponding `apigen/api/*.openapi.yaml` and generated package directory.
- Pure runtime changes usually do not touch `apigen/` at all.

## 3) Runtime serving path to Swagger

Swagger/OpenAPI serving flow:

1. Server startup calls `docs.RegisterSwaggerRoutes(api)`.
2. `GET /index/swagger` returns the Swagger UI page.
3. The UI loads `GET /index/openapi.yaml`.
4. `GET /index/openapi.yaml` is built by merging runtime spec documents:
   - required: `openapi.yaml` (DRS), `lfs.openapi.yaml`
   - optional: `bucket.openapi.yaml`, `metrics.openapi.yaml`, `internal.openapi.yaml`, `compat.openapi.yaml`
5. Spec files are read from embedded assets first via `apigen/api/specs_embed.go` (`ReadSpec`), with filesystem fallback for local dev.

### Sanity checks

- `make gen` or the specific `make gen-*` target should complete without leaving unexpected files under `.tmp/`.
- `go test ./cmd/server ./tests/endpoints/...` should pass for DRS route and coverage changes.
- `go test ./...` is the broadest check, but route/spec changes should also be validated against the affected docs endpoints:
  - `/index/swagger`
  - `/index/openapi.yaml`
  - `/index/lfs.openapi.yaml`
  - `/index/bucket.openapi.yaml`
  - `/index/internal.openapi.yaml`

## 4) Confirming currently pinned GA4GH schema version

Run from repo root:

```bash
# authoritative submodule pointer committed in superproject
git ls-tree HEAD ga4gh/data-repository-service-schemas

# branch hint configured for the submodule
cat .gitmodules

# initialize or refresh local submodule checkout
make init-schemas

# actual checked-out commit in local submodule worktree
git -C ga4gh/data-repository-service-schemas rev-parse HEAD
```

At the time this guide was updated:

- Superproject pinned commit: `935a20952e1071421c28d569b8c8e0e940bc001f`
- `.gitmodules` branch hint: `feature/get-by-checksum`

That means Syfon is not currently pinned by default to `feature/issue-416-drs-upload`.

## 5) How to change GA4GH schema version (example: `feature/issue-416-drs-upload`)

1. Initialize submodule if needed:

   ```bash
   make init-schemas
   ```

2. Switch submodule to the desired branch/commit:

   ```bash
   cd ga4gh/data-repository-service-schemas
   git fetch origin
   git checkout feature/issue-416-drs-upload
   # optional hard pin:
   # git checkout <exact-commit-sha>
   cd ../..
   ```

3. Regenerate all OpenAPI outputs:

   ```bash
   make gen
   ```

4. Commit both submodule pointer and regenerated artifacts:

   ```bash
   git add ga4gh/data-repository-service-schemas apigen
   git commit -m "Pin GA4GH schemas to feature/issue-416-drs-upload and regenerate OpenAPI outputs"
   ```

## 6) Post-change validation checklist

After schema/codegen changes:

- Run tests (`go test ./...` or at least impacted API/doc packages).
- Verify docs endpoints:
  - `GET /index/swagger`
  - `GET /index/openapi.yaml`
  - optional direct specs: `GET /index/lfs.openapi.yaml`, `GET /index/bucket.openapi.yaml`, `GET /index/internal.openapi.yaml`
- Confirm final submodule pointer is what you intended via `git ls-tree HEAD ga4gh/data-repository-service-schemas`.

## 7) Requested GA4GH reference branch

- <https://github.com/ga4gh/data-repository-service-schemas/tree/feature/issue-416-drs-upload>
