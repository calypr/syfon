# Problem / Solution: Organization and Project Storage Metrics

## Problem

The frontend organization explorer wants compact summary statistics at the organization and project level, especially:

- total file count per project
- total stored bytes per project
- organization-level rollups across accessible projects

A frontend workaround that pages through `GET /index?organization=...&project=...` for every visible project and then sums record sizes in JavaScript is not sustainable.

Problems with the frontend approach:

- request volume grows linearly with the number of visible projects
- large projects require walking every page of the index listing just to compute a summary row
- page load time and UI responsiveness become coupled to full index traversal
- multiple clients would independently repeat the same aggregation work
- the summary is conceptually a metrics concern, not a file-listing concern

## Desired Outcome

Syfon should expose organization- and project-scoped aggregate metrics so clients can request summary stats directly without enumerating every record.

At minimum, the metrics surface should support:

- exact project summaries for `{organization, project}`
- organization rollups across all projects in that organization
- total file count
- total stored bytes

## Proposed Direction

Add org/project filters and aggregate responses to the Syfon metrics API.

Possible shapes:

1. Extend an existing metrics endpoint to accept `organization` and optional `project` filters.
2. Add a dedicated metrics endpoint for storage summaries, for example a summary route under the metrics namespace.
3. Support grouped responses so one request can return all project summaries for an organization.

Example response shape:

```json
{
  "organization": "aced",
  "project": "evotypes",
  "file_count": 12842,
  "total_bytes": 9412381234
}
```

Example grouped organization response:

```json
{
  "organization": "aced",
  "projects": [
    {
      "project": "evotypes",
      "file_count": 12842,
      "total_bytes": 9412381234
    }
  ],
  "organization_totals": {
    "file_count": 12842,
    "total_bytes": 9412381234
  }
}
```

## Data Semantics

Open questions that should be decided explicitly:

- Whether `total_bytes` is the sum of indexed record sizes, object-store bytes, or another canonical storage measure.
- Whether deleted, superseded, or non-current versions should contribute to totals.
- Whether metrics should be computed from the index, persisted as pre-aggregated counters, or refreshed asynchronously.
- Whether authorization should filter the response to only scopes visible to the caller, or whether this endpoint is only used for already-authorized org/project scopes.

## Why This Belongs in Metrics

This is shared aggregate data with stable semantics and broad reuse potential.

Potential consumers include:

- organization/project explorer pages
- administrative dashboards
- quota and storage reporting
- operational monitoring

A metrics endpoint keeps listing endpoints focused on record retrieval and keeps clients from rebuilding the same aggregation logic independently.

## Acceptance Criteria

- A client can request project-level totals without traversing every `/index` page.
- A client can request organization-level totals without traversing every project.
- Returned values have documented semantics for file count and bytes.
- The response is fast enough to support summary rows in interactive UIs.

## Current Status

Frontend aggregate-size display has been removed for now. The organization explorer should only render data that can be fetched efficiently until a Syfon metrics implementation exists.
