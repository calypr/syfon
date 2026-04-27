# Provider Billing Logs

Syfon billing metrics use provider-observed transfer logs. For cloud buckets, the same credential registered with Syfon must be able to read the configured log bucket and prefix. Bucket registration fails when `billing_log_bucket` or `billing_log_prefix` is missing or unreadable.

The sync operation validates and imports whatever log files are present. A brand-new log prefix may be readable but empty; in that case Syfon records a completed sync with an empty-import warning. Metrics dashboards still return persisted metrics and include freshness metadata (`is_stale`, `missing_buckets`, `latest_completed_sync`) so users can see whether the numbers are current.

## AWS S3

Enable S3 server access logging on the data bucket and deliver logs to a bucket/prefix that the Syfon credential can list/read.

```bash
aws s3api put-bucket-acl \
  --bucket "$LOG_BUCKET" \
  --acl log-delivery-write

aws s3api put-bucket-logging \
  --bucket "$DATA_BUCKET" \
  --bucket-logging-status "{
    \"LoggingEnabled\": {
      \"TargetBucket\": \"$LOG_BUCKET\",
      \"TargetPrefix\": \"$LOG_PREFIX\"
    }
  }"
```

Register the bucket with the same log location:

```bash
syfon bucket add "$DATA_BUCKET" \
  --provider s3 \
  --region "$AWS_REGION" \
  --access-key "$AWS_ACCESS_KEY_ID" \
  --secret-key "$AWS_SECRET_ACCESS_KEY" \
  --billing-log-bucket "$LOG_BUCKET" \
  --billing-log-prefix "$LOG_PREFIX"
```

Syfon imports S3 server access log records for object GET/PUT-style operations and normalizes transferred bytes, requester, key, status, and request ids. Use real server access log files as fixtures when validating a deployment; CloudTrail/CUR/Storage Lens exports are different formats and should not be pointed at this prefix unless normalized first.

## Google Cloud Storage

Create a Cloud Logging sink that exports Cloud Storage data access logs to a GCS bucket/prefix readable by the Syfon service account. Data Access logs must be enabled for Cloud Storage.

```bash
gcloud logging sinks create syfon-gcs-transfer-logs \
  "storage.googleapis.com/$LOG_BUCKET/$LOG_PREFIX" \
  --log-filter='resource.type="gcs_bucket" AND protoPayload.serviceName="storage.googleapis.com"'

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$(gcloud logging sinks describe syfon-gcs-transfer-logs --format='value(writerIdentity)' | sed 's/serviceAccount://')" \
  --role="roles/storage.objectCreator"
```

Register the bucket:

```bash
syfon bucket add "$DATA_BUCKET" \
  --provider gcs \
  --secret-key "$GOOGLE_APPLICATION_CREDENTIALS_JSON" \
  --billing-log-bucket "$LOG_BUCKET" \
  --billing-log-prefix "$LOG_PREFIX"
```

Syfon reads exported JSON log entries and JSON arrays/envelopes from Cloud Logging sinks and normalizes object read/write operations when byte fields are present in the exported records. Data Access logs can omit byte counts depending on export shape; records without byte fields are imported as zero-byte observations and should be investigated before billing.

## Azure Blob Storage

Enable diagnostic settings for the blob service and route `StorageRead`, `StorageWrite`, and `StorageDelete` logs to a storage account/container readable by the Syfon credential.

```bash
az monitor diagnostic-settings create \
  --name syfon-blob-transfer-logs \
  --resource "$BLOB_SERVICE_RESOURCE_ID" \
  --storage-account "$LOG_STORAGE_ACCOUNT_ID" \
  --logs '[
    {"category":"StorageRead","enabled":true},
    {"category":"StorageWrite","enabled":true},
    {"category":"StorageDelete","enabled":true}
  ]'
```

Register the container:

```bash
syfon bucket add "$DATA_CONTAINER" \
  --provider azure \
  --access-key "$AZURE_STORAGE_ACCOUNT" \
  --secret-key "$AZURE_STORAGE_KEY" \
  --endpoint "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net" \
  --billing-log-bucket "$LOG_CONTAINER" \
  --billing-log-prefix "$LOG_PREFIX"
```

Syfon reads Azure resource log JSON records and `records` envelopes and normalizes blob read/write operations with response/request byte counts.

## Sync

After logs are configured and the bucket is registered:

```bash
syfon metrics transfers sync \
  --provider s3 \
  --bucket "$DATA_BUCKET" \
  --from "2026-04-26T00:00:00Z" \
  --to "2026-04-27T00:00:00Z"
```

Metrics summary and breakdown endpoints always return the persisted provider ledger. For bounded windows, inspect the `freshness` block in the response:

- `is_stale: false` means a completed provider sync covers the requested provider/bucket/time range.
- `is_stale: true` means at least one provider/bucket is missing completed sync coverage.
- `missing_buckets` lists the missing provider/bucket pairs.
- `latest_completed_sync` shows the most recent completed sync time Syfon knows about.

Provider sync writes structured logs with provider, bucket, prefix, window, imported, matched, ambiguous, unmatched, duration, and any warning/error so operators can alert on stale or empty imports.
