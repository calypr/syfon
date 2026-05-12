# Credential Encryption

This document describes the supported operator-facing credential encryption model for Syfon.

Syfon encrypts persisted bucket credentials such as `access_key` and `secret_key` at rest.

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

## Supported Mode

For operator docs, the supported mode is the local KEK flow. Syfon keeps a server-side key-encryption key and uses it to wrap the per-secret data keys used for stored bucket credentials.

## Local KEK Mode

In local mode, Syfon uses a server-managed local KEK file.

Key file path resolution:

- `DRS_CREDENTIAL_LOCAL_KEY_FILE` if set
- else `dirname(DRS_DB_SQLITE_FILE)/.syfon-credential-kek` if `DRS_DB_SQLITE_FILE` is set
- else `/app/.syfon-credential-kek`

Behavior:

- If key file exists, it is loaded.
- If missing, Syfon generates a new 32-byte key and writes it with restrictive permissions.

Optional override:

- `DRS_CREDENTIAL_MASTER_KEY` can explicitly set the local KEK.
- Accepted formats:
  - a 32-character raw string
  - a 64-character hex string
  - a base64-encoded 32-byte key

## Operational Notes

- Clients do not need to provide encryption keys.
- Changing HTTP basic-auth credentials does not change the local KEK when using the default local key-file behavior.
- Keep the local KEK file persisted with your DB volume in local deployments.
- If you lose the KEK that was used to encrypt stored bucket credentials, Syfon will no longer be able to decrypt those credentials.
