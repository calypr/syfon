# Final internal hardening plan

## Execution brief

Finish the remaining correctness and cleanup work on existing PR [#129](https://github.com/calypr/syfon/pull/129). Start from `refactor/arc-16`, currently `ca7e6b7`, or a verified descendant. Append commits to this branch. Do not rebase the stack, modify earlier branches, create another PR, or merge.

The prepared tip worktree is `/private/tmp/syfon-refactor-arc-16`. Reuse it after checking its status. The main `/Users/peterkor/Desktop/BMEG/syfon` worktree is on another branch with unrelated untracked files; do not switch or clean it. If the prepared path is unavailable, locate the branch with `rtk git worktree list` before doing anything else.

This document is a plan, not authorization to start implementation in the planning session. Invoking the executor prompt below approves its proposed attribution policy.

Target approximately 20 minutes of implementation and local verification using one Sol medium executor and three Luna xhigh workers. Remote CI can finish later. This is a target, not a guarantee. Do not omit required tests or ship an incomplete security fix to meet it.

Use the current filenames, not the older file listing in the conversation. Preserve database schemas, stored records, HTTP routes, generated bindings, token algorithms, issuer restrictions, signing behavior, and transaction boundaries. The intentional behavior changes are key-cache reuse and reliable attribution of new events. Do not rewrite historical events or grant IDs.

## Ownership and sequence

| Lane | Exclusive production ownership | Result |
| --- | --- | --- |
| A: authentication | `internal/access/authentication/**` | Runtime-owned discovery/key cache |
| B: attribution | `internal/transfers/**`, `internal/httpapi/transfers/**` | Explicit or unambiguous event scope |
| C: cleanup | `internal/buckets/**`, `internal/objects/**`, `internal/httpapi/buckets/**`, `internal/httpapi/maintenance/**`, the two persistence `content_identity.go` files | Shared policy/calculation and operation-based file splits |
| Sol executor | Integration, documentation, any unforeseen external caller, final review and push | One integrated candidate |

Workers may add tests beside their production files. B may also add dedicated `attribution_scope_test.go` files in SQLite and PostgreSQL; it must not edit C's persistence files or shared test helpers. C may add dedicated metadata-merge tests in either adapter. No other worker touches those tests.

- Minutes 0–2: Sol checks the worktree and remote tip, assigns all three lanes, and records worker status in one short local note. No fresh architectural panel or broad baseline rerun.
- Minutes 2–14: workers implement and run their focused checks. Use disjoint files in one worktree, with Sol as the sole Git writer. Report changed files, tests, and blockers. Do not modify another lane's files.
- Minutes 14–20: Sol reviews the combined diff once, integrates caller corrections, runs the final checks, commits, and pushes one batch. Recheck only packages affected by review corrections.
- If behind schedule, defer C3's mechanical splits first and record that explicitly. Never leave duplicate old/new implementations, skip a required regression test, or claim the whole packet is complete when a lane is unfinished.

## A. Authentication cache lifetime

### Evidence and shape

`token_auth.go:Resolve` calls `parseToken`. In `token.go`, each verification performs discovery and constructs a new `jwksCache`. Its 15-minute TTL cannot provide cross-request reuse. `Runtime` already owns a long-lived `tokenAuthResolver`; put verifier/cache ownership there, within the existing package.

### Implementation

1. Make token verification use a resolver-owned verifier. Retain keys and the discovered JWKS URL across requests for the allowed issuer. Check the issuer allowlist before discovery or cache access; a key ID alone is not a cache key.
2. Reuse the existing 15-minute lifetime. Serialize refresh so concurrent requests do not all fetch keys. Inject the clock and HTTP client privately for tests; use request context and a bounded production HTTP timeout.
3. On an unknown key ID, permit one refresh attempt subject to a short per-issuer cooldown, including unsuccessful refresh attempts. Use a named default such as 30 seconds. One request must not repeatedly refresh, and random unknown IDs must not trigger an unbounded series of fetches.
4. Replace the cached key set only after a successful fetch and decode. Do not extend the TTL on failures. Preserve failure on expired keys that cannot refresh, unknown keys, invalid signatures, disallowed issuers, and insecure JWKS endpoints. Do not add stale-key acceptance.
5. Migrate the existing `parseToken` tests to the instance-owned verifier. Do not keep a production wrapper that constructs a fresh verifier per call. Keep the existing privilege lookup and authentication/authorization decision flow unchanged; this is not a token-result or privilege cache.

### Required proof

Use the existing TLS/JWT test fixtures, counting discovery and JWKS requests. Prove repeated and concurrent verification reuse keys; expiry refreshes; key rotation refreshes; unknown-key floods are bounded; failed refresh does not accept an invalid token; issuer/key-ID isolation holds. Preserve the existing issuer, algorithm, signature, and expiry tests. Avoid sleeps and new global transport mutation in the added tests.

Run `go test -race ./internal/access/authentication`. Report request-count assertions, not a claim that latency was benchmarked.

## B. Transfer attribution scope

### Evidence and proposed policy

`transfers/access_events.go:scopeForAccess` returns the first entry of `Record.Authorizations`. The existing probe produced three different scopes for the same record. Organization/project participate in `usage.GrantID` and `usage.EventID`. Several DRS, LFS, and ordinary download paths have no requested project, so sorting the map is not a correct fix.

Use this policy for new events:

- A scope explicitly established by the authorized operation takes precedence.
- Otherwise, infer a scope only when the record has exactly one distinct canonical scope. Use `objects.AccessResources`, including its controlled-access precedence, rather than reading the authorization map independently.
- Multiple or no attributable scopes produce empty organization/project fields. Keep the event and the successful transfer. Do not duplicate the event across projects or infer the billed project from a shared bucket, map order, or the caller's permission set.
- Ambiguous events will not enter a specific project's totals. They remain stored for appropriately authorized aggregate reporting. Do not broaden report access to make them visible.

### Implementation

1. Add a small typed optional scope to `transfers.AccessRequest`. Use absence to mean no operation-selected scope; do not add a schema column or API query parameter.
2. In the existing explicitly scoped upload path, pass the scope only after the current target resolution and authorization have succeeded. Validate/canonicalize through existing resource parsing and access rules. Raw organization/project request strings are not trusted attribution. For existing-record access, the selected scope must also agree with the record's canonical resources; an unsupported attribution hint stays unattributed rather than inventing ownership or changing transfer permission.
3. Keep DRS/LFS/unscoped paths on the unambiguous-record fallback. Update the relevant existing `RecordAccessIssued` call sites and tests. Preserve signing, lookup, accounting, and error order. Do not rewrite target selection or split attribution into another package.
4. Resolve the scope before computing event/grant IDs. Keep those ID algorithms unchanged. Persisted historical IDs stay untouched.

### Required proof

Table-test single scope, two projects in one organization, multiple organizations, duplicate resources, explicit valid scope, unsupported/unauthorized hint, no scope, and controlled-access precedence. Assert exact expected results, not a probabilistic map-iteration test. With fixed request identity, prove equivalent inputs yield the same grant/event IDs. Prove an ambiguous download still succeeds and emits one event with empty scope.

Add small adapter tests showing empty-scope events are retained and excluded from project-filtered totals. Cover SQLite behavior and PostgreSQL SQL arguments. Do not alter schema or report filtering.

Run `go test ./internal/transfers/... ./internal/httpapi/transfers ./internal/persistence/sqlite ./internal/persistence/postgres`.

## C. Bounded cleanup

### C1. One owner for bucket authorization

`httpapi/buckets/policy.go` and `httpapi/maintenance/authorization.go` duplicate scope authorization, including organization-level Arborist exceptions. Move the shared scope policy into `internal/buckets`, which already depends on `access`. Leave transport-specific header rejection in the handlers and call the shared policy directly. Do not create `apiutil`, generic policy adapters, or forwarding wrappers.

Preserve local enforcement-off behavior, Gen3 missing-header rejection, project methods, wildcard methods, and `arborist:create-descendant`/`manage-owners` exceptions. Add table tests and retain the existing route tests, including their status codes.

### C2. Share only the pure registration metadata calculation

SQLite's `mergeContentRowTx` and PostgreSQL's `postgresMergeContentRowTx` repeat the same calculation for name, version, description, size, updated time, and the name alias to preserve.

Extract that calculation into `internal/objects/registration_merge.go`. A small value input/result containing those fields is sufficient. The result contains merged metadata and an optional name alias; it does not perform SQL, mutate inputs, read context, or authorize anything. Both adapters invoke it using state already loaded inside their transactions.

Pin these existing rules before replacing the duplicate calculation:

- Replacement is allowed only when the current resource set has exactly one entry and overlaps the incoming resources.
- A changed name preserves the old name as an alias when replacing, otherwise preserves the incoming name as an alias.
- Names retain current basename normalization. Blank incoming fields do not erase populated values.
- A nonzero stored size is retained. A later incoming updated timestamp wins.

Do not reuse `MergeRecordUpdate` blindly; patch/update semantics differ from registration merge semantics. Keep each adapter's alias INSERT and metadata UPDATE in the same order and transaction. Leave identity lookup, UUID claims, SHA conflicts, scope authorization, public-read policy, SQL placeholders, and locks unchanged. Do not extract the rest of `registerContentTx` in this packet.

Use pure table tests plus existing SQLite identity tests and PostgreSQL mutation/SQL tests. Add one adapter assertion per dialect if existing tests do not observe the merged values and preserved alias.

### C3. Mechanical file splits, last priority

If C1/C2 are green with time remaining, move whole declarations within their existing packages:

- `httpapi/maintenance/inspect.go`: object inspection, project/bucket inspection, request/response types and conversion.
- `objects/records/listing.go`: scoped listing, pagination, checksum queries, and authorization-filter helpers.
- `objects/records/canonicalization.go`: pure canonical view merging versus repository-backed duplicate repair/registration operations.

Use existing destination files where suitable and concrete names. Do not change signatures, exports, query ports, fallback behavior, or function bodies. Verify declaration/token equivalence, allowing import and formatting changes. Do not delete apparently dead compatibility paths without a separate call-site assessment.

Run `go test ./internal/buckets ./internal/objects/... ./internal/httpapi/buckets ./internal/httpapi/maintenance ./internal/persistence/sqlite ./internal/persistence/postgres`.

## Final gate and delivery

Sol reviews once for scope correctness, cache refresh/error behavior, policy parity, and unchanged transaction boundaries. Read worker test evidence rather than reflexively rerunning every focused suite. Workers do not review one another or request Astra.

From the integrated repository, run these once:

```sh
rtk proxy env GOCACHE=/tmp/syfon-refactor-arc-14-final-cache go test ./internal/... ./cmd/server ./tests/endpoints
rtk proxy env GOCACHE=/tmp/syfon-refactor-arc-14-final-cache go vet ./internal/... ./cmd/server
rtk proxy env GOCACHE=/tmp/syfon-refactor-arc-14-final-cache bash scripts/check_internal_import_policy.sh
rtk proxy bash scripts/check_internal_import_policy.sh --self-test
rtk git diff --check
```

The authentication race check is required in addition to those commands. Confirm no schema/generated/client changes and no unintended edits to prior work. Reuse the route inventory tests included in `cmd/server`; do not add a second identical gate. If a check fails, diagnose it and rerun the affected check after correction.

Append focused commits and push once to `origin/refactor/arc-16`. Update PR #129 with behavior changes, tests, and any explicitly deferred C3 work. Remote CI owns Docker, live PostgreSQL, Calypr, and final Codecov calculations. Do not lower coverage thresholds, bypass failures, or claim remote checks passed before observing them. Leave the PR unmerged.

## Execution status — 2026-09-07

A, B, C1, and C2 are implemented on the existing `refactor/arc-16` tip. Three
disjoint Luna workers supplied implementation and focused tests; the executor
performed one integrated review. That review required stronger rotation,
issuer-isolation, authorization, and metadata/alias assertions and removed a
redundant JWKS fetch within one verification.

C3 is explicitly deferred: `httpapi/maintenance/inspect.go` and
`objects/records/{listing,canonicalization}.go` have not been mechanically split
in this hardening batch. No schema, generated binding, client, historical event,
or attribution-ID algorithm changes are included.

Verification passed with `GOCACHE=/tmp/syfon-refactor-arc-14-final-cache`:

- All three lanes' focused suites, including both persistence adapters.
- `go test -race ./internal/access/authentication` after the review corrections.
- One integrated `go test ./internal/... ./cmd/server ./tests/endpoints`.
- `go vet ./internal/... ./cmd/server`.
- Import policy, its self-tests, and `git diff --check`.

Request-count assertions cover concurrent cache reuse, key rotation, failed and
successful refresh cooldowns, and issuer isolation. These are correctness tests,
not a latency benchmark. Remote Docker, live PostgreSQL, Calypr, and Codecov checks
remain CI responsibilities on the new head. Delivery is recorded in PR #129.

## Copyable Sol executor prompt

```text
Use /private/tmp/syfon-refactor-arc-16 and execute
docs/internal-final-hardening-plan.md on the existing refactor/arc-16 tip
for PR #129. I approve its proposed ambiguous-attribution policy: retain
the event without project attribution, without failing the transfer.

You are the Sol medium executor. Launch three bounded Luna xhigh workers for
A authentication, B attribution, and C cleanup using the file ownership in
the plan. Do not launch architectural panels or more reviewers. Keep a short
status note and be the sole Git writer. Prefer disjoint files in the existing
tip worktree; preserve unrelated user changes.

Target 20 minutes, but do not skip required tests, weaken security, or push
unfinished work to meet the target. C3 mechanical splits are the first scope
to defer if necessary; report deferrals explicitly. Perform one integrated
review, run the final gates once, append commits, and push one batch to
origin/refactor/arc-16. Update existing PR #129. No rebase, new PR, migration,
historical attribution rewrite, merge, or changes to earlier stack branches.
```
