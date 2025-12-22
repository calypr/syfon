# drs-server

A lightweight reference implementation of a GA4GH Data Repository Service (DRS) server in Go.

## Table of Contents
- [Overview](#overview)
- [Quickstart](QUICKSTART.md)
- [Contributing](CONTRIBUTING.md)
- [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Overview

GA4GH DRS is a standard API for describing and accessing data objects in cloud or on‑premise repositories.  
This project consumes the official GA4GH `data-repository-service-schemas` as a Git submodule and generates a Go HTTP server from the DRS OpenAPI spec.


```mermaid
graph TD
  0[ga4gh/data-repository-service-schemas DRS OpenAPI spec submodule] --> B
  A[Makefile] --> B[make gen]
  A --> C2[cmd/server]
  A --> D[make test]
  A --> E[make docs/]

  B --> G[internal/apigen generated DRS server code]
  B --> H[cmd/openapi-remove-examples clean OpenAPI helper] --> H2[internal/apigen/api/openapi.yaml]

  H2 --> C2
  D --> C2
  G --> C2
```

* Makefile - targets for generation, tests, docs, and running the server.
  * `make gen` - generates the DRS server code from the OpenAPI spec.
    * ga4gh/data-repository-service-schemas - GA4GH DRS OpenAPI spec (Git submodule).
    * internal/apigen - generated DRS server code.
    * cmd/openapi-remove-examples - helper to clean the bundled OpenAPI.
  * `make serve` - runs the DRS server.
    * cmd/server - main HTTP server (uses gin-gonic/gin).
  * `make test` - launches server, runs integration tests.
  * `make docs` - serves documentation with MkDocs.

