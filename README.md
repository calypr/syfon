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


## Key files

```mermaid
graph TD
  A[Makefile] --> B[make gen]
  A --> C[make serve]
  A --> D[make test]
  A --> E[make docs]

  B --> F[ga4gh/data-repository-service-schemas<br/>(DRS OpenAPI spec submodule)]
  B --> G[internal/apigen<br/>(generated DRS server code)]
  B --> H[cmd/openapi-remove-examples<br/>(clean OpenAPI helper)]

  C --> I[cmd/server<br/>(main HTTP server using gin-gonic/gin)]

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

