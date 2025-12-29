# drs-server

A lightweight reference implementation of a GA4GH Data Repository Service (DRS) server in Go.

## Table of Contents
- [Overview](#overview)
- [Quickstart](QUICKSTART.md)
- [Contributing](CONTRIBUTING.md)
- [GitHub Pages](https://calypr.github.io/drs-server/)
- [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Overview

GA4GH DRS is a standard API for describing and accessing data objects in cloud or on‑premise repositories.  
This project consumes the official GA4GH `data-repository-service-schemas` as a Git submodule and generates a Go HTTP server from the DRS OpenAPI spec.


```mermaid
flowchart TB
  Spec[OpenAPI Spec Git Submodule] --> CI
  CI[CI make gen] --> Contract[Bundle Spec, Model, Handler stubs]
  Contract --> MiddlewareChain
  Contract --> Report[Fail build on violations]

  subgraph MiddlewareChain[Middleware Chain]
    LogReq[Logging redact auth] --> ReqVal[OpenAPI Request Validation enforce]
    ReqVal --> Handler
    Handler --> RespVal[OpenAPI Response Validation audit or enforce]
    RespVal --> Commit
  end

  MiddlewareChain --> Gin[GIN HTTP Server]


```

* Makefile \- targets for generation, tests, docs, and running the server.
  * `make gen` \- generates the DRS server code from the OpenAPI spec and cleans bundled examples.
    * `ga4gh/data-repository-service-schemas` \- GA4GH DRS OpenAPI spec (Git submodule).
    * `internal/apigen` \- generated DRS server code.
    * `cmd/openapi-remove-examples` \- helper to clean the bundled OpenAPI.
  * `make serve` \- runs the DRS server (`cmd/server`) with optional `ARGS` passed through.
  * `make test` \- cleans the Go test cache and runs all tests with verbose output.
  * `make docs-image` \- builds the Docker image used for MkDocs docs (`Dockerfile.mkdocs` to `mkdocs-material-mermaid:latest`).
  * `make docs` \- serves the documentation locally with MkDocs in Docker on port `8000`.
  * `make docs-build` \- builds the static MkDocs site into the local `site` directory using Docker.
