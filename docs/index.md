# drs-server

A lightweight, production-grade implementation of a GA4GH Data Repository Service (DRS) server.

## Overview

`drs-server` provides:

- GA4GH DRS-compatible metadata and object access APIs.
- SQLite for local development 
- PostgreSQL for deployed Gen3 mode
- S3-compatible signed URL access for object download and multipart upload workflows.
- Compatibility endpoints used by existing pipelines.

Start with [Quickstart](quickstart.md), then use [Build](build.md) for software build and code generation steps, and [Configuration](configuration.md) for mode-specific settings.

See [Architecture](architecture.md) for package layout and the system view diagram.

## Development and Standards

- Development commands and test workflows are documented in [Development](development.md).
- Internal-vs-standards architectural rationale is documented in [ADR](adr-internal-vs-ga4gh-drs.md).
