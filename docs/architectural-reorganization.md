# Architectural Reorganization Proposal

This document outlines a proposed reorganization of the Syfon codebase to improve maintainability, clarify boundaries, and follow standard Go project layouts.

## Rationale

The current structure has several organizational challenges:
1. **Top-level Fatigue**: Too many directories at the root level (20+ folders).
2. **Infrastructure Leakage**: Modules like `db`, `urlmanager`, and `provider` are top-level and exposed, rather than being encapsulated.
3. **Fragmented Logic**: Business logic is split between `service/`, `internal/api/...`, and `internal/coreapi/`.
4. **SDK Placement**: The public-facing SDK (`client/`) is at the root, making it harder to distinguish from internal server code.

## Proposed Layout

### 1. Unified `internal/` Core
Most of the current top-level folders should be subordinate to a unified `internal/` package to signal they are not public APIs.

- **`internal/service/`**: Unified business logic layer. Merges `service/`, `internal/coreapi/`, and core logic from handlers.
- **`internal/infra/`**: Encapsulated infrastructure implementations.
  - `internal/infra/db/`: Moving `db/` here.
  - `internal/infra/storage/`: Merging `urlmanager/` and `internal/provider/`.
- **`internal/handler/`**: Consolidated HTTP entry points.
  - `internal/handler/drs/`: GA4GH standard handlers.
  - `internal/handler/internalapi/`: Syfon-specific management APIs.
  - `internal/handler/lfs/`: Git LFS handlers.
  - `internal/handler/metrics/`: Metrics and logging handlers.
- **`internal/gen/`**: Moving `apigen/` here to house generated code.

### 2. Public Facing Packages
- **`pkg/client/`**: Move the `client/` directory here. This is the idiomatic location for libraries intended to be imported by external projects.

### 3. Cleanup
- **`api/types/`**: Delete. These are currently redundant type aliases. The application should use unified domain models or refer to generated models directly.
- **`adapter/`**: Move to `internal/service/adapter/` or incorporate into the service layer.

## File Move Map

| Current Path | New Path | Rationale |
| :--- | :--- | :--- |
| `service/` | `internal/service/` | Keep logic internal. |
| `db/` | `internal/infra/db/` | Clear infrastructure boundary. |
| `urlmanager/` | `internal/infra/storage/` | Infrastructure for object storage. |
| `adapter/` | `internal/service/adapter/` | Specific to service mapping. |
| `client/` | `pkg/client/` | Publicly importable SDK. |
| `internal/api/...` | `internal/handler/...` | Differentiate logic from handlers. |
| `apigen/` | `internal/gen/` | Hide generated glue code. |

## Implementation Strategy

1. **Phase 1: Tooling**: Use `gofmt` and `goimports` to handle the large volume of import updates.
2. **Phase 2: Infrastructure Move**: Move `db` and `urlmanager` first, as they have the fewest internal dependencies.
3. **Phase 3: Logic Move**: Migrate `service/` and update handlers.
4. **Phase 4: Handler Relocation**: Move HTTP handlers to `internal/handler`.
5. **Phase 5: SDK Move**: Relocate `client/` to `pkg/client` and update the root `go.mod` replace directives.
