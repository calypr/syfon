# ADR: Use GA4GH DRS for the standard contract and internal routes for Syfon workflows

## Status

Accepted

## Context

The GA4GH DRS API covers object metadata, access methods, and standard object operations. Syfon also needs server workflows for scoped indexing, bucket routing, uploads, multipart sessions, and transfer metrics. Those workflows carry state and policy that the base DRS contract does not describe.

## Decision

Use the GA4GH DRS routes for standard DRS operations. Keep Syfon-specific operations under the internal route groups and expose their OpenAPI contract in `apigen/openapi/internal.openapi.yaml`.

The internal API owns operations such as:

- scoped index listing and record mutations;
- bulk hash checks and document operations;
- bucket and project storage inspection;
- upload URL and multipart session workflows;
- ranged download URL signing;
- transfer and usage metrics.

New clients should use the GA4GH route when it provides the required operation. Use an internal route when the operation depends on Syfon storage layout, project scope, multipart state, or an internal reporting model.

## Boundary rules

The two route groups share authentication, authorization, storage, and persistence services. They do not share wire types by accident. Generated models for the DRS schema stay in `apigen/server/drs` and `apigen/client/drs`. Generated models for Syfon-specific contracts stay in the corresponding internal packages.

An internal endpoint must document its request and response shape in the local OpenAPI file and must preserve the route's scope and authorization checks. Removing an internal endpoint requires migrating its callers and the integration tests that cover it.

## Consequences

Syfon can provide a conforming DRS surface while keeping durable upload, transfer, and project workflows explicit. The repository carries two related contracts, so code generation and endpoint tests must cover both. The internal API remains an implementation contract for Syfon clients and is not presented as a GA4GH standard.
