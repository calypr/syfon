# ADR: Mediate data transfer through Syfon

## Status

Accepted

## Context

Syfon supports several storage providers. Letting the client hold provider credentials would duplicate provider logic in the CLI and make authorization and transfer accounting harder to enforce.

## Decision

Syfon mediates object transfers through its HTTP API:

1. The server resolves the object and storage target.
2. The server checks the request's authentication and authorization context.
3. The server asks the provider adapter to sign a short-lived URL.
4. The client transfers bytes over the signed URL with standard HTTP.

The client can request signed URLs for single transfers, multipart upload parts, and inclusive byte ranges. Provider SDKs remain in the server storage adapters. The client does not need cloud credentials for these operations.

## Consequences

The server is in the path for every transfer authorization and can record access events with object, scope, provider, bucket, and byte-range metadata. The client stays provider-neutral, but transfers depend on the Syfon server being available long enough to issue the signed URL.

The server remains responsible for provider-specific signing behavior. For example, ranged GCS URLs include the `Range` header in the signature, while S3 and Azure use their provider-specific signing APIs.

## Related code

- `internal/transfers` resolves objects and signs access requests.
- `internal/storage` selects the provider adapter and performs signing.
- `client/transfer` drives HTTP downloads and multipart operations.
- `internal/usage` and `internal/transfers` record access events.
