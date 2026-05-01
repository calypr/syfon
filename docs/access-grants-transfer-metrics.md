# Access Grants and Transfer Metrics

Syfon records transfer billing telemetry when it successfully issues signed upload
and download URLs.

## Concepts

- `access_grant` is the canonical billing identity for a signed or direct storage access authorization. A grant is keyed by object DID, SHA256, organization, project, access ID, provider, bucket, and storage URL.
- `access_issued` is an append-only audit event recorded every time Syfon returns an access URL. Transfer totals are computed from these signed-url rows.
- Transfer summaries expose aggregate `bytes_downloaded` and `bytes_uploaded`.
- Transfer breakdowns can group those bytes by scope, user, storage provider/bucket, or object SHA256.

## Billing Report

Use the combined CLI report when you need the billing shape used by dashboards:

```bash
syfon metrics transfers billing --organization cbds --project end_to_end_test
```

The response includes:

- `summary`: total download and upload event counts and byte totals.
- `storage_locations`: provider/bucket rows with download and upload bytes.
- `files`: object SHA256 rows with download and upload bytes.

The same data is also available as separate calls:

```bash
syfon metrics transfers summary
syfon metrics transfers breakdown --group-by provider
syfon metrics transfers breakdown --group-by object
```

When authz is enforced and no explicit organization/project filter is provided,
transfer metrics aggregate across the project scopes the caller can read.

## Operational Notes

- No provider access-log sync is required for signed-url billing metrics.
- The old provider transfer sync API is retained for compatibility and diagnostics,
  but the CLI no longer exposes sync commands for normal billing workflows.
- Blank uploads and multipart part signing cannot always be attributed to a DRS
  object at URL-issue time; object-backed upload/download URL signing is recorded
  with file, scope, provider, bucket, and byte metadata.
