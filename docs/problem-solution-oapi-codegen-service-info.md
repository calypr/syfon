# Problem and Solution: GA4GH Service Info Codegen Type Errors

## Problem

The updated GA4GH DRS schema can make `make gen` succeed while `make build` fails with errors like:

```text
undefined: N200ServiceInfoDrsControlledAccessClaimFormat
undefined: N200ServiceInfoDrsControlledAccessDefault
undefined: N200ServiceInfoDrsSupportedUploadMethodTypes
undefined: DrsServiceDrsSupportedUploadMethods
```

This is a Go code generation problem, not a runtime DRS behavior problem.

The failure comes from the `/service-info` response. In DRS, `/service-info` is the endpoint that advertises server capabilities, such as supported upload methods, delete support, and controlled-access support.

The schema for that response is composed from two schemas:

```yaml
200ServiceInfo:
  content:
    application/json:
      schema:
        allOf:
          - $ref: ga4gh-service-info Service
          - $ref: DrsService
```

The updated `DrsService` schema adds or renames enum-like fields under `drs`, including:

```yaml
supportedUploadMethodTypes:
  type: array
  items:
    type: string
    enum:
      - s3
      - gs
      - https
      - ftp
      - sftp

controlledAccessClaimFormat:
  type: string
  enum:
    - ga4gh-passport-url-claim

controlledAccessDefault:
  type: string
  enum:
    - open-access-read
```

Those fields are valid OpenAPI, but they are awkward for `oapi-codegen` because they are inline enums inside a nested object inside a composed response.

## Why Build Fails

`oapi-codegen` generates the real enum types using the named schema path:

```go
type DrsServiceDrsControlledAccessClaimFormat string
type DrsServiceDrsControlledAccessDefault string
type DrsServiceDrsSupportedUploadMethodTypes string
```

But in the generated `/service-info` response structs, it references response-specific names:

```go
ControlledAccessClaimFormat *N200ServiceInfoDrsControlledAccessClaimFormat
ControlledAccessDefault *N200ServiceInfoDrsControlledAccessDefault
SupportedUploadMethodTypes *[]N200ServiceInfoDrsSupportedUploadMethodTypes
```

Those `N200ServiceInfo...` types are not emitted by the generator, so Go compilation fails.

There is also a rename involved. Older generated code used:

```go
DrsServiceDrsSupportedUploadMethods
```

The updated schema uses:

```go
DrsServiceDrsSupportedUploadMethodTypes
```

That makes the old handwritten compatibility alias point at a type that no longer exists.

## Easy Fix Without Changing the GA4GH Schema

Keep the GA4GH schema as-is and update the handwritten `compat.go` aliases to match the new generated type names.

For the client package:

```go
type N200ServiceInfoDrsControlledAccessClaimFormat = DrsServiceDrsControlledAccessClaimFormat
type N200ServiceInfoDrsControlledAccessDefault = DrsServiceDrsControlledAccessDefault
type N200ServiceInfoDrsSupportedUploadMethodTypes = DrsServiceDrsSupportedUploadMethodTypes
type N200ServiceInfoTypeArtifact = DrsServiceTypeArtifact
```

For the server package, include both the normal response and strict JSON response aliases:

```go
type N200ServiceInfoDrsControlledAccessClaimFormat = DrsServiceDrsControlledAccessClaimFormat
type N200ServiceInfoDrsControlledAccessDefault = DrsServiceDrsControlledAccessDefault
type N200ServiceInfoDrsSupportedUploadMethodTypes = DrsServiceDrsSupportedUploadMethodTypes
type N200ServiceInfoTypeArtifact = DrsServiceTypeArtifact

type N200ServiceInfoJSONResponseDrsControlledAccessClaimFormat = DrsServiceDrsControlledAccessClaimFormat
type N200ServiceInfoJSONResponseDrsControlledAccessDefault = DrsServiceDrsControlledAccessDefault
type N200ServiceInfoJSONResponseDrsSupportedUploadMethodTypes = DrsServiceDrsSupportedUploadMethodTypes
type N200ServiceInfoJSONResponseTypeArtifact = DrsServiceTypeArtifact
```

For the shared model package, add the same non-JSON-response aliases:

```go
type N200ServiceInfoDrsControlledAccessClaimFormat = DrsServiceDrsControlledAccessClaimFormat
type N200ServiceInfoDrsControlledAccessDefault = DrsServiceDrsControlledAccessDefault
type N200ServiceInfoDrsSupportedUploadMethodTypes = DrsServiceDrsSupportedUploadMethodTypes
type N200ServiceInfoTypeArtifact = DrsServiceTypeArtifact
```

Also remove or replace old aliases for `DrsServiceDrsSupportedUploadMethods`, because that old type name is no longer generated after the schema rename.

## Why This Fix Works

Go type aliases make the generated response-specific names resolve to the actual generated enum types.

This does not change the wire API. JSON still uses the same fields:

```json
{
  "drs": {
    "supportedUploadMethodTypes": ["s3", "https"],
    "controlledAccessClaimFormat": "ga4gh-passport-url-claim",
    "controlledAccessDefault": "open-access-read"
  }
}
```

The aliases only repair the Go package's type graph after code generation.

## Tradeoffs

This is the fastest and lowest-risk fix when the source GA4GH schema cannot be changed.

Pros:

- No GA4GH schema changes.
- No OpenAPI overlay changes.
- Small handwritten patch.
- Keeps generated client, server, and model packages compiling.

Cons:

- It keeps depending on `oapi-codegen` naming behavior.
- Future inline enum additions under `/service-info` may need more aliases.
- `make gen` can still produce code that requires these compatibility files to compile.

## More Durable Fixes

If schema changes are allowed later, the cleaner fix is to avoid inline enums for these fields.

Define named schemas:

```yaml
DrsControlledAccessClaimFormat:
  type: string
  enum:
    - ga4gh-passport-url-claim

DrsControlledAccessDefault:
  type: string
  enum:
    - open-access-read

DrsSupportedUploadMethodType:
  type: string
  enum:
    - s3
    - gs
    - https
    - ftp
    - sftp
```

Then reference them:

```yaml
controlledAccessClaimFormat:
  $ref: './DrsControlledAccessClaimFormat.yaml'

controlledAccessDefault:
  $ref: './DrsControlledAccessDefault.yaml'

supportedUploadMethodTypes:
  type: array
  items:
    $ref: './DrsSupportedUploadMethodType.yaml'
```

That gives `oapi-codegen` stable names and removes the need for response-specific alias patches.

If upstream GA4GH cannot change, Syfon could also normalize the schema through a local OpenAPI overlay before code generation. That is more maintainable than chasing aliases forever, but it is more work than the immediate compatibility fix.

## Recommended Short-Term Policy

For this branch:

1. Keep the GA4GH schema unchanged.
2. Update `apigen/*/compat.go` aliases for the new generated names.
3. Run `make build`.
4. Commit regenerated API files and compatibility aliases together.

If another schema update later adds more inline enums under `/service-info`, expect the same class of failure and either add aliases or move those enums to named schemas.
