# drs-server

A lightweight, production-grade implementation of a GA4GH Data Repository Service (DRS) server in Go.

## Purpose

The `drs-server` provides a robust implementation of the [GA4GH DRS API](https://ga4gh.github.io/data-repository-service-schemas/). It is designed to manage metadata for data objects and provide secure access via signed URLs.

### Key Features
- **GA4GH DRS Compliance**: Implements the standard DRS API for describing and accessing data objects.
- **Database Flexibility**: Supports both **SQLite** and **PostgreSQL** backends with a modular driver architecture.
- **S3 Integration**: Native support for Amazon S3 (and compatible storage like MinIO) with signed URL generation for downloads and multipart uploads.
- **Gen3 Compatibility Layers**:
    - **Fence Compatibility**: Supports Fence-style `/data/download` and multipart upload endpoints.
    - **Indexd Compatibility**: Provides Indexd-style metadata management for integration with `git-drs`.

## Configuration

The server is configured via a YAML or JSON file. Use the following structure to set up your environment:

```yaml
port: 8080

database:
  sqlite:
    file: "drs.db"
  # Or use PostgreSQL:
  # postgres:
  #   host: "localhost"
  #   port: 5432
  #   user: "user"
  #   password: "password"
  #   database: "drs"
  #   sslmode: "disable"

s3_credentials:
  - bucket: "my-test-bucket"
    region: "us-east-1"
    access_key: "AKIAXXXXXXXXXXXXXXXX"
    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    endpoint: "s3.amazonaws.com" # Optional: set for MinIO or custom backends
```

## Running Integration Tests

You can run integration tests using your own config file:

```bash
go test ./cmd/server -v -count=1 -testConfig=config.yaml
```

## Architecture

The project follows a modular structure to ensure maintainability:
- `db/core`: Core interfaces and models.
- `db/sqlite`, `db/postgres`: Database implementation drivers.
- `internal/api`: Subpackages for different API contexts (Admin, Fence, Gen3).
- `service`: High-level business logic implementing the DRS service.
- `urlmanager`: Logic for interacting with cloud storage providers.

## Development

The project uses a Makefile for common tasks:
- `make gen`: Generates the DRS server code from the official GA4GH OpenAPI spec (Git submodule).
- `make test`: Runs all unit and integration tests.
- `make serve`: Starts the DRS server.


