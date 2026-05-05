# Local Basic Authz CSV

Syfon local auth mode can use a CSV file to make Basic Auth behave like a
small method-aware authorization system. This is intended for local development
and CI tests that need to exercise the same read/write authorization checks used
in Gen3 mode, without decoding Gen3 tokens or calling Fence/Arborist services.

If no CSV is configured, set `auth.basic.username/password` for the legacy
single-user admin/unrestricted local mode. Fully unauthenticated local mode
requires the explicit development-only opt-in `auth.allow_unauthenticated: true`.

## Configure

Use `auth.local_authz_csv` in the server config:

```yaml
port: 8080

auth:
  mode: local
  local_authz_csv: "./local-authz.csv"

database:
  sqlite:
    file: "./drs.db"

s3_credentials:
  - bucket: "syfon-data"
    provider: "s3"
    region: "us-east-1"
    access_key: "local"
    secret_key: "local"
    endpoint: "http://localhost:9000"
```

Or set the equivalent environment variable:

```bash
export DRS_AUTH_MODE=local
export DRS_LOCAL_AUTHZ_CSV=./local-authz.csv
```

When `DRS_LOCAL_AUTHZ_CSV` or `auth.local_authz_csv` is set, Basic Auth users
come from the CSV. `auth.basic.username/password` and
`DRS_BASIC_AUTH_USER/PASSWORD` are not used for authentication.

## CSV Format

Use `organization,project` columns when you want Syfon to build Gen3-style
resource paths:

```csv
username,password,organization,project,methods
alice,alice-pass,cbds,end_to_end_test,read
bob,bob-pass,cbds,end_to_end_test,read|write
carol,carol-pass,ohsu,release_1,read,file_upload
```

Use a `resource` column when you already have the full resource path:

```csv
username,password,resource,methods
alice,alice-pass,/programs/cbds/projects/end_to_end_test,read
bob,bob-pass,/programs/cbds/projects/end_to_end_test,read|write
ops,ops-pass,/services/internal/buckets,read|write
```

Accepted header aliases:

- User: `username`, `user`, or `subject`
- Password: `password` or `pass`
- Methods: `methods`, `permissions`, or `access`
- Resource: `resource`, `path`, or `authz_path`
- Organization: `organization`, `org`, or `program`
- Project: `project` or `project_id`

Separate multiple methods with `|`, `;`, or `,`.

The `write` alias expands to:

- `file_upload`
- `create`
- `update`
- `delete`

Use `*` when a local test needs all methods for a resource.

## Behavior

Each request must use HTTP Basic Auth credentials from the CSV:

```bash
curl -u alice:alice-pass \
  "http://localhost:8080/ga4gh/drs/v1/objects/<object_id>"
```

When a request is authenticated, Syfon injects the CSV resources and methods
into the same method-aware authorization path used by Gen3-mode handlers. That
means local CSV authz can test read/write decisions for:

- DRS object reads and signed download URLs
- Internal upload/download URL signing
- Delete/update/create flows
- Metrics queries scoped to readable projects
- Bucket control paths when the CSV grants `/services/internal/buckets`

If a user is not present in the CSV, authentication fails. If the user is
present but lacks the required method/resource, the request is denied.

## Example Users

For a local test cluster with two users:

```csv
username,password,organization,project,methods
reader,reader-pass,cbds,end_to_end_test,read
writer,writer-pass,cbds,end_to_end_test,read|write
```

Expected behavior:

- `reader` can fetch DRS objects and signed download URLs for
  `/programs/cbds/projects/end_to_end_test`.
- `reader` cannot create, update, delete, or request upload URLs for that
  project.
- `writer` can read and perform upload/create/update/delete checks for that
  project.
- Neither user can access other projects unless another CSV row grants them.

## Notes

- This feature is for local mode. Gen3 mode still uses the Gen3 authn/authz
  integration.
- Without `auth.local_authz_csv` or `DRS_LOCAL_AUTHZ_CSV`, local Basic Auth is
  single-user admin mode for backward compatibility.
- Do not commit real passwords. Use test-only credentials in repository fixtures
  and CI configs.
