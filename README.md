# drs-server

A lightweight reference implementation of a GA4GH Data Repository Service (DRS) server in Go.

## What is GA4GH DRS?

The GA4GH Data Repository Service (DRS) is a standard API for accessing data objects in cloud and on-premise repositories.  
DRS provides a uniform way to:

* Describe data objects (size, checksums, access URLs, metadata).
* Retrieve access URLs for objects, regardless of where they are stored.
* Decouple *how* data is stored from *how* it is accessed by clients.

The official specification and schemas live in the GA4GH `data-repository-service-schemas` repository and are consumed here to generate server stubs and API documentation.

## Repository layout

Key components:

* `cmd/server` \- main HTTP server using `gin-gonic/gin`.
* `internal/apigen` \- generated server code from the DRS OpenAPI spec.
* `cmd/openapi-remove-examples` \- helper to strip problematic examples from the bundled OpenAPI.
* `Makefile` \- entrypoint for code generation, running tests, serving docs, and running the server.
* `ga4gh/data-repository-service-schemas` \- Git submodule containing the official DRS OpenAPI spec and schemas.

## OpenAPI schemas as a Git submodule

This project uses the official GA4GH DRS schemas as a Git submodule so that:

* The server stays closely aligned with the upstream spec.
* Schema updates can be pulled in explicitly and reproducibly.
* Code generation is driven directly from the upstream OpenAPI document.

The submodule is expected at a path similar to:

* `ga4gh/data-repository-service-schemas`

To initialize and update the submodule:

```bash
git submodule update --init --recursive
```

When the upstream DRS spec is updated and you want to consume the latest version:

```bash
cd ga4gh/data-repository-service-schemas
git fetch origin
git checkout <tag-or-branch>
cd -
git add ga4gh/data-repository-service-schemas
git commit -m "Update DRS schemas to <tag-or-branch>"
```

The Makefile uses the OpenAPI file from this submodule (by default):

* `OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml`

You can override this path when running generation (see below).

## Quickstart

### Prerequisites

* Go 1.22\+ installed.
* Docker installed (for OpenAPI generation and docs).
* Git submodules initialized:

```bash
git submodule update --init --recursive
```


### Generate API stubs from OpenAPI

By default this uses the DRS OpenAPI file from the Git submodule:

```bash
make gen
```

You can override the OpenAPI file path:

```bash
make gen OPENAPI=/path/to/custom/openapi.yaml
```

This will:

* Regenerate `internal/apigen` from the OpenAPI spec using `openapi-generator`.
* Run `cmd/openapi-remove-examples` to clean up non\-compliant examples.

### Run tests

```bash
make test
```

### Run the server

```bash
make serve
```

By default, this will:

* Build and run the server from `cmd/server`.
* Listen on the configured address/port (check `cmd/server` for defaults or flags).

You should then be able to reach basic endpoints such as:

* `/healthz` \- simple health check.
* `/service-info` \- service metadata (name, version, timestamp).
* 🚧 DRS endpoints as defined in the generated handlers once implemented.

### Serve documentation with MkDocs

API and project documentation can be served locally using MkDocs Material in Docker:

```bash
make docs
```

This will:

* Mount the repository into the MkDocs container.
* Serve docs on `http://localhost:8000`.

## Development workflow

1. Update or pin the DRS OpenAPI spec (via the submodule).
2. Run `make gen` to regenerate server stubs.
3. Implement or update handlers in `cmd/server` and supporting packages.
4. Run `make test` and `make serve` to validate behavior.
5. Optionally run `make mkdocs` to verify or update documentation.

## Contributing

Contributions are welcome. Please see `CONTRIBUTING.md`


## License

This project is licensed under the terms described in `LICENSE` in the project root.

By contributing to this repository, you agree that your contributions will be licensed under the same terms.
