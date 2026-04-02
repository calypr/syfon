# Credential Encryption

This document describes how Syfon currently encrypts bucket credentials (`access_key`, `secret_key`) at rest.

## Design

Syfon uses envelope encryption (`enc:v2`):

1. Generate a random 32-byte DEK per encrypted field write.
2. Encrypt plaintext with DEK using AES-GCM.
3. Wrap the DEK with a key manager KEK.
4. Store an envelope containing:
   - key manager name
   - key identifier metadata
   - wrapped DEK
   - AES-GCM nonce
   - ciphertext
5. On read, unwrap DEK through the manager and decrypt.

Legacy `enc:v1` values are still readable for backward compatibility.

## Key Managers

Syfon supports:

- `local` (default)
- `aws-kms`

Selection rules:

- If `DRS_CREDENTIAL_KEY_MANAGER` is set, that manager is used.
- Else if `DRS_CREDENTIAL_KMS_KEY_ID` is set, `aws-kms` is used.
- Else `local` is used.

## Local Mode (Default)

In local mode, Syfon uses a server-managed local KEK file.

Key file path resolution:

- `DRS_CREDENTIAL_LOCAL_KEY_FILE` if set
- else `dirname(DRS_DB_SQLITE_FILE)/.syfon-credential-kek` if `DRS_DB_SQLITE_FILE` is set
- else `/tmp/.syfon-credential-kek`

Behavior:

- If key file exists, it is loaded.
- If missing, Syfon generates a new 32-byte key and writes it with restrictive permissions.

Optional override:

- `DRS_CREDENTIAL_MASTER_KEY` can explicitly set the local KEK.

## AWS KMS Mode

Required:

- `DRS_CREDENTIAL_KMS_KEY_ID`
- IAM permissions for `kms:Encrypt` and `kms:Decrypt`
- AWS credentials/role and region (`AWS_REGION` or `AWS_DEFAULT_REGION`)

When enabled, DEKs are wrapped/unwrapped through AWS KMS.

## Operational Notes

- Clients do not need to provide encryption keys.
- Changing HTTP basic-auth credentials does not change the local KEK when using the default local key-file behavior.
- Keep the local KEK file persisted with your DB volume in local deployments.
