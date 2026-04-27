# Access Grants and Transfer Metrics

Syfon separates authorization telemetry from billable transfer usage.

## Concepts

- `access_grant` is the canonical billing identity for a signed or direct storage access authorization. A grant is keyed by object DID, SHA256, organization, project, access ID, provider, bucket, and storage URL.
- `access_issued` is an append-only audit event recorded every time Syfon returns an access URL. It proves authorization was issued, not that bytes moved.
- `provider_transfer_event` is provider-observed storage activity imported from S3, GCS, Azure, or file-provider logs. These events are the source of truth for billable upload and download bytes.

## Reconciliation

When provider transfer events are imported, Syfon reconciles each event to an `access_grant`.

1. If the provider event includes an `access_grant_id`, Syfon matches that grant directly.
2. Otherwise Syfon matches by provider, bucket, object key or storage URL, and the access grant issue time window.
3. A single match is marked `matched`.
4. No match is marked `unmatched`.
5. Multiple different candidate grants are marked `ambiguous`.

Billing summaries and breakdowns count matched provider transfer events only. `access_issued` audit rows are useful for debugging and visibility, but they are never counted as completed transfer usage by themselves.

## Why Signed URLs Are Not Usage

Issuing a signed URL only means Syfon authorized a client to access a storage object. The client might never use the URL, might read a partial range, might retry, or might receive an error from the provider. Provider logs are required to know which bucket request actually happened and how many bytes moved.

## Operational Notes

- Configure provider log collection for each bucket before using transfer metrics for billing.
- Use provider transfer sync metadata to identify the latest completed sync window.
- Missing or stale sync windows should be shown to dashboard users so they know whether reported usage is complete for the selected time range.
