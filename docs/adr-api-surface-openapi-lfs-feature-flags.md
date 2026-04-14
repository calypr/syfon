# ADR: API Surface Inventory (OpenAPI + LFS) and Startup Feature Flags

## Status
Proposed

## Date
2026-04-14

## Context
Syfon currently exposes multiple API surfaces in one server process:
- GA4GH DRS endpoints
- Internal DRS compatibility endpoints
- Metrics endpoints
- Bucket endpoints
- Git LFS endpoints

At runtime, we also expose multiple OpenAPI documents (plus one merged document) through docs routes. This is useful but not explicitly documented in one place, and operators do not currently have first-class startup toggles to enable or disable specific API families.

## Decision Drivers
- Make API ownership and discoverability clearer for users and operators.
- Reduce accidental API exposure by allowing opt-in/opt-out at startup.
- Keep startup behavior explicit, auditable, and easy to reason about.
- Preserve current default behavior for backward compatibility.

## API Surface Inventory

### OpenAPI Documents Used by the Server

The docs router publishes these OpenAPI routes:
- Merged API spec: `GET /index/openapi.yaml`
- LFS spec: `GET /index/openapi-lfs.yaml`
- Bucket spec: `GET /index/openapi-bucket.yaml`
- Internal spec: `GET /index/openapi-internal.yaml`
- Swagger UI: `GET /index/swagger` and `GET /index/swagger/`

Route registration source:
- `internal/api/docs/swagger.go` (`RegisterSwaggerRoutes`)
- `config/config.go` route constants for docs routes

Spec composition behavior (high level):
- `openapi.yaml` is the base.
- The merged route adds paths/components from `lfs.openapi.yaml`.
- It conditionally merges `bucket.openapi.yaml`, `metrics.openapi.yaml`, and `internal.openapi.yaml` when available.
- It conditionally merges `compat.openapi.yaml` when available.

### LFS API Surface

LFS routes currently exposed by default:
- `POST /info/lfs/objects/batch`
- `POST /info/lfs/objects/metadata`
- `PUT /info/lfs/objects/{oid}`
- `POST /info/lfs/verify`

Route registration source:
- `internal/api/lfs/lfs.go` (`RegisterLFSRoutes`)
- `config/config.go` LFS route constants

Server startup wiring source:
- `cmd/server/server.go` currently always registers LFS routes after core API route setup.

## User Stories (High Level)

### OpenAPI Multi-Spec User Stories
- As an API consumer, I want a single merged spec endpoint so I can generate clients with one URL.
- As a platform integrator, I want per-domain specs (LFS, bucket, internal) so I can scope integration and validation to a targeted API family.
- As an operator, I want docs routes to reflect enabled APIs so published contracts match runtime availability.

### LFS API User Stories
- As a Git user, I want standard Git LFS batch and object transfer endpoints so existing `git lfs` tooling works without custom clients.
- As a repository admin, I want to disable LFS in environments where only DRS workflows are supported.
- As a security/compliance operator, I want explicit startup control over LFS exposure to reduce attack surface and meet policy.

## Decision
Introduce startup feature flags that control API-family registration, starting with OpenAPI docs publishing and LFS route exposure.

### Proposed Startup Flags
Configuration-level booleans (exact naming can be finalized during implementation):
- `features.docs_enabled` (default: `true`)
- `features.lfs_enabled` (default: `true`)
- `features.internal_api_enabled` (default: `true`)
- `features.metrics_enabled` (default: `true`)

Optional environment-variable overrides (illustrative):
- `DRS_FEATURE_DOCS_ENABLED`
- `DRS_FEATURE_LFS_ENABLED`
- `DRS_FEATURE_INTERNAL_API_ENABLED`
- `DRS_FEATURE_METRICS_ENABLED`

### Startup Behavior (Proposed)
At server bootstrap, each API family is registered only when its corresponding feature flag is enabled.

Pseudo-flow:
1. Load config and resolve feature flags.
2. Register core GA4GH DRS routes (always on).
3. Conditionally register docs routes.
4. Conditionally register internal routes.
5. Conditionally register metrics routes.
6. Conditionally register LFS routes.
7. Log enabled/disabled feature families at startup.

### OpenAPI/Docs Behavior with Flags
- If `features.docs_enabled=false`, docs and raw spec routes are not registered.
- If docs are enabled but a dependent API family is disabled (for example LFS), the merged spec should omit that family's paths/components.
- This keeps the contract surface aligned with runtime behavior.

## Consequences

### Positive
- Clear and explicit API-surface governance.
- Safer production posture via least-exposed interfaces.
- Better operator ergonomics for phased rollout and deprecation.

### Trade-offs
- More config combinations to test.
- Potential confusion if clients expect an endpoint family that is disabled.
- Additional release notes and migration guidance needed.

## Rollout Plan (High Level)
1. Add `features` config section and env var overrides.
2. Wire conditional route registration in `cmd/server/server.go`.
3. Update docs route merge logic to respect enabled families.
4. Add startup logs summarizing enabled API families.
5. Add matrix tests for enabled/disabled combinations.
6. Document defaults and operational examples in README/docs.

## Alternatives Considered
- **No flags (status quo):** simplest, but does not address exposure control.
- **Build-time tags only:** too inflexible for runtime operations.
- **Separate binaries for each API family:** strongest isolation, but significantly increases operational complexity.

## Out of Scope
- Per-route authorization policy redesign.
- Multi-tenant dynamic runtime toggling without restart.
- Breaking changes to existing route paths.

