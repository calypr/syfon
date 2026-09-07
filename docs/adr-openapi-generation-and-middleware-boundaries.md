# ADR: OpenAPI generation and runtime boundaries

## Status

Accepted

## Context

Syfon has generated API contracts, an HTTP request pipeline, and domain services. Keeping those responsibilities separate lets the OpenAPI documents change without moving authorization or storage policy into generated code.

## Decision

Use three boundaries:

1. The generation layer defines request and response shapes, generated models, and typed server interfaces.
2. The HTTP layer registers routes and applies authentication, authorization, request limits, error mapping, and logging.
3. Domain and adapter packages enforce object, transfer, storage, and persistence rules.

Syfon uses `oapi-codegen` with Fiber server templates. The generated packages under `apigen/server` and `apigen/client` are contract code. The handwritten server under `internal/httpapi` owns route registration and adapters. The `internal/access`, `internal/transfers`, `internal/storage`, and `internal/persistence` packages own runtime behavior.

## Error ownership

| Failure | Owner |
| --- | --- |
| Malformed route or request shape | Generated decoder or HTTP adapter |
| Missing or invalid identity | Authentication middleware |
| Insufficient permission | Authorization middleware or domain policy |
| Unknown object after a valid request | Domain service |
| Storage, database, or provider failure | Owning adapter and HTTP error mapper |

The generated contract does not decide authorization. A schema change does not replace the runtime checks that protect objects and storage operations.

## Code-generation contract

Run `make gen` after changing the canonical DRS schema, a local OpenAPI document, or a code-generation config. Commit generated files with the input change. Keep generated-shape work at the API boundary and adapt it before it reaches domain code.

## Consequences

The generated API remains a stable contract for clients and server adapters. Middleware can enforce deployment-specific policy without changing the schema. Domain packages stay testable without Fiber or generated request types. Changes that cross boundaries need focused tests at each affected layer.
