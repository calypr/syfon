# QA remediation plan

This is the implementation companion to [qa-bug-ledger.md](qa-bug-ledger.md). The ledger is the source of truth for status and evidence; this file specifies the smallest intended fix and an observable completion check for every open finding. It is written for sequential implementation by a coding agent. Do not treat a passing existing test as proof of a fix when the ledger's failing edge case is absent from that test.

## Working rules

1. Start each cluster from the current working tree, inspect its diff, and preserve unrelated edits. The repository is already modified. Load `golang-patterns` before Go changes and prefix shell commands with `rtk`.
2. For each ID, reproduce the ledger trigger with a focused regression check where practical, make the smallest behavior change at the owning boundary, then rerun that check and the package plus direct importers. Keep probes synthetic and avoid printing credentials or signed URL query strings.
3. Implement cohesive clusters below as separate reviewable changes. Run the full repository suite once after integrating a cluster. Tests that bind loopback listeners require a runner where that is permitted. Claims about PostgreSQL or cloud-provider behavior require live integration in CI or a configured test environment. A fake transport can prove client-side request construction or routing, but must not be presented as proof that the backend accepts or processes the request.
4. Server bootstrap and configuration changes must continue to render and run with `/Users/peterkor/Desktop/gen3-helm/helm/syfon`. Run the focused Helm config test, `helm lint`, and chart render tests for those changes. The chart currently requires a stable credential key by default.
5. Close a ledger row only after the named observable result is verified. When a row names a conditional product contract, use the decision recorded below, update the API docs/schema, and test that contract. Do not add compatibility layers without a current caller.

QA-044 closes the remaining open rows from this remediation plan. The ledger has no open items. QA-001, QA-002, QA-003, QA-004, QA-005, QA-006, QA-007, QA-008, QA-009, QA-010, QA-011, QA-012, QA-013, QA-014, QA-016, QA-017, QA-018, QA-019, QA-020, QA-021, QA-022, QA-023, QA-024, QA-025, QA-026, QA-027, QA-028, QA-029, QA-031, QA-032, QA-033, QA-034, QA-035, QA-036, QA-037, QA-038, QA-039, QA-040, QA-041, QA-042, QA-043, QA-044, QA-045, QA-046, QA-047, QA-048, QA-049, QA-050, QA-051, QA-052, QA-053, QA-054, QA-055, QA-056, QA-057, QA-058, and QA-059 are fixed; QA-015 and QA-030 were rejected after falsification.

## A. Access and credential boundaries

No open items remain in this cluster.

## B. Durable object and usage state

| ID | Implementation | Completion check |
| --- | --- | --- |
No open items remain in this cluster.

## C. Transfer and storage behavior

| ID | Implementation | Completion check |
| --- | --- | --- |

## D. CLI and project copy

| ID | Implementation | Completion check |
| --- | --- | --- |
| QA-016 | When `syfon download` chooses a default output name, derive a safe basename from the record name and reject empty, absolute, or parent-component results. Keep explicit `--out` as the caller's chosen path. | Focused helper tests show traversal and absolute names resolve to safe basenames, while empty, dot, parent, and root-only candidates fail. The command keeps its DID fallback for blank record names and applies the helper only to its default-name branch; explicit `--out` bypasses it. |
| QA-017 | Replace `os.ReadFile`/`io.ReadAll` hashing in `syfon upload` and `syfon sha256sum` with streaming `io.Copy` into SHA-256. For HTTP inputs, close the body and enforce the request context; do not materialize the whole object. | Hash a 16 MiB file to the expected digest. A synthetic 4 MiB HTTP body rejects reads above 64 KiB and still hashes correctly; cancellation and body closure are verified. The existing CLI output path is unchanged. |
| QA-023 | Have `NewServerClient` refresh an expired profile token through `EnsureUsableProfileCredential` when a usable API key exists, then persist the refreshed credential before creating the client. Keep explicit token-only profiles working. | A temporary profile with expired token and valid API key causes one refresh and the next CLI request uses the new token; a valid token causes no refresh. |
| QA-024 | In `copy-project` scope resolution, keep destination bucket and its organization path in one value rather than overwriting them independently during map iteration. Remap project paths against that selected bucket and fail before `AddScope` if translation is impossible. | With two destination org scopes and an untranslatable source path, the command returns a deterministic validation error without creating a mismatched scope; a translatable path creates the intended scope regardless of map order. |
| QA-025 | Register `validate` flags with Cobra at command construction, before Cobra parses arguments. Remove the late standard `flag` registration inside `Run`. | `syfon validate --addr ... --debug` reaches the validator without `unknown flag`; invalid values still return a command error. |
| QA-026 | Return `app.Listen` errors from the `validate` command rather than logging and returning nil. Keep one clear error message at the CLI boundary. | A fake or occupied listener causes nonzero command exit; a successful listener lifecycle remains successful. |
| QA-027 | Treat `total_users` as the total number of users matching filters before `--limit`, and keep the users array limited. Document that meaning in the metrics response and CLI help. If product intent is only returned rows, rename the field instead of calling it total. | With three matching users and limit one, response has one row and `total_users=3`; no-limit output remains unchanged. |
| QA-035 | For the server's unsupported storage-deletion capability, make `syfon rm` request metadata deletion without `delete_storage_data=true`; adjust its success text to match the actual action. If supporting other DRS servers, consult service-info before requesting a storage purge. | The command now sends `delete_storage_data:false`, reports that storage data is preserved, and leaves the multi-resource scoped-access branch unchanged. The DRS route regression passes; the command regression asserts request and output but its loopback listener was denied by the sandbox. |

## E. Server, config, and API contracts

| ID | Implementation | Completion check |
| --- | --- | --- |
| QA-007 | In `decodeStrictJSON`, require the second decoder call to return exactly `io.EOF`. Return a bad-request error for any trailing token or malformed trailing bytes. Leave one-value JSON requests accepted. | A mutation request with a valid object followed by malformed text returns 400 with no mutation; valid single JSON and whitespace-only trailing input work. |
| QA-009 | Classify PostgreSQL startup ping errors by cause. Retry transient connection failures within the startup window, but fail fast on permanent authentication/authorization errors such as rejected credentials. Preserve the configured window for genuine unavailability. | Synthetic `pq.Error` and network cases cover SQLSTATE and connection classification. A conditional live PostgreSQL test supplies an invalid password and verifies prompt failure with SQLSTATE `28P01`. |
| QA-010 | Thread the startup context through the PostgreSQL constructor and store auto-bootstrap path. Replace background `PingContext`/schema work and unbounded `Exec` with context-aware calls; apply a finite DDL timeout for non-production auto-bootstrap. Production schema checks already have a bound. | Driver tests prove caller cancellation and the configured DDL deadline reach bootstrap. A conditional PostgreSQL integration test holds an `ACCESS EXCLUSIVE` lock and verifies auto-bootstrap exits within its configured deadline. |
| QA-012 | Bound the cleanup interval, inactivity timeout, and completed retention seconds before converting them to `time.Duration`; keep timeout/interval relationships valid. | `LoadConfig` rejects one-above-maximum values for all three fields and accepts the maximum; the reconciler starts with the maximum valid interval. |
| QA-034 | In master-key parsing, accept an exactly 32-byte raw value when Base64 decoding succeeds to the wrong length, after unambiguous hex/valid 32-byte Base64 cases. Avoid logging the key. | A 32-byte raw key made of Base64 alphabet characters encrypts data and works through configured-bucket startup and credential persistence; valid hex and Base64 keys still work, and invalid lengths fail. |
| QA-036 | Make bulk DRS deletion atomic as the published contract prefers: validate every requested physical ID and delete authority before any mutation, then recheck and delete the set in one transaction. Return 404 for a missing member and 403 for an unauthorized member. | Mixed valid/missing and valid/unauthorized requests preserve every record and return the matching 4xx; a valid multi-object batch returns 204 and deletes all. |
| QA-037 | Accept the schema-declared `hashes` field as an alias for `sha256` in bulk validity requests. Normalize at the HTTP boundary, reject an empty request, and define deterministic handling when both fields are present. | Generated-client requests using either field, or identical dual-field arrays, return equivalent validity maps. Missing, empty, whitespace-only, or conflicting values receive a documented 400. |
| QA-044 | Validate stability against the selected credential key manager, not merely any nonempty KMS key ID. If `local` is explicit, require a stable master key or persisted local key file for production; a KMS ID alone must not satisfy validation. | Production config with explicit local manager plus only a KMS ID fails before startup; a configured stable local key and the Helm chart defaults still pass. A KMS-selected manager with its key ID remains valid. |

## Delivery and sign-off

Implement one package-owned cluster at a time. QA-037 and QA-044 are fixed, and no remediation rows remain open. For any new open row, record the focused test command and result, then run its direct importers. Run `git diff --check`, the relevant API schema generation check, and the full Go suite once after integration. State any sandbox-blocked tests explicitly; do not call a source-only review a runtime reproduction.

The QA-027 contract is settled: `total_users` counts all matching users before the CLI limit. QA-052 now rejects `contents` and `mime_type` until the server can persist them. If product intent differs, change the corresponding contract and regression check before implementation.

## Handoff prompt for a Luna Max implementation run

> For any new open row in `docs/qa-remediation-plan.md`, read the matching ledger row and current source/diff before editing. Apply `golang-patterns` and the repository's RTK instruction. Reproduce the stated wrong result with synthetic data, make the smallest fix at the named boundary, and verify the listed completion checks plus package and direct-importer tests. Preserve unrelated working-tree changes and the Helm chart contract. Update each ledger row to Fixed only after its check passes. Report exact files changed, test commands/results, and any integration check that could not run. Stop after that cluster is reviewable; choose the next cluster only after that result is checked.

For later runs, replace the two IDs in that prompt with the next package-owned group. Keep the ledger and this plan in the context so the agent can see the exact trigger and acceptance check.
