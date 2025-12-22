**Explanation**

It covers project scope, how to set up the repo (including submodules), common `make` targets, coding style, tests, and licensing of contributions.

```markdown
# Contributing to `drs-server`

Thank you for your interest in contributing to `drs-server`, a lightweight reference implementation of a GA4GH Data Repository Service (DRS) server in Go.

This document outlines how to set up your environment, make changes, and submit them.

## Code of conduct

Be respectful and constructive. Assume good faith in discussions and reviews.

## Project overview

`drs-server`:

* Implements a GA4GH DRS\-compatible HTTP API in Go.
* Uses the official GA4GH DRS OpenAPI spec via a Git submodule at `ga4gh/data-repository-service-schemas`.
* Generates server stubs into `internal/apigen` from that OpenAPI spec.

## Getting started

### Fork and clone

1. Fork the repository on GitHub.
2. Clone your fork:

   ```bash
   git clone git@github.com:<you>/drs-server.git
   cd drs-server
   ```

3. Initialize submodules:

   ```bash
   git submodule update --init --recursive
   ```

### Branching

Create a feature branch off `main`:

```bash
git checkout -b feature/<short-description>
```

Use short, descriptive names, e.g. `feature/add-service-info`, `fix/healthz-handler`.

## Development workflow

See QUICKSTART.md for details on common tasks.

### Updating the OpenAPI spec

If your change requires a newer DRS spec:

```bash
cd ga4gh/data-repository-service-schemas
git fetch origin
git checkout <tag-or-branch>
cd -
git add ga4gh/data-repository-service-schemas
git commit -m "Update DRS schemas to <tag-or-branch>"
```

Then regenerate:

```bash
make gen
```

Commit both the submodule update and any generated code.

## Coding guidelines

### Go style

* Follow standard Go conventions (`gofmt`, idiomatic naming).
* Keep handlers and business logic small and testable.
* Prefer composition over inheritance\-like patterns.

Before committing, run:

```bash
gofmt -w ./cmd ./internal
go test ./...
```

🧪🧪🧪🧪 Update the docs/

### Generated code

* Do not manually edit files under `internal/apigen`.
* Regenerate using `make gen` when the OpenAPI spec changes.
* Commit the regenerated files along with the spec or handler changes.

## Testing

* Add or update tests alongside your changes.
* Ensure `make test` passes before opening a pull request.
* For new endpoints or behaviors, include tests that cover both success and error paths.

## Documentation

* Update `README.md` or docs (served via `make mkdocs`) when public behavior or endpoints change.
* Keep examples minimal, accurate, and aligned with the DRS spec.

## Submitting changes

1. Ensure your branch is up to date with `main`.
2. Run:

   ```bash
   make test
   ```

3. Push your branch:

   ```bash
   git push -u origin feature/<short-description>
   ```

4. Open a pull request against the upstream `main` branch.
5. In the PR description, explain:
    * What changed.
    * Why it is needed.
    * Any implications for API, configuration, or deployment.

Address review feedback with additional commits; avoid force\-pushing unless requested.

## License and contribution terms

By contributing to this repository, you agree that your contributions will be licensed under the terms of the project license described in `LICENSE`.