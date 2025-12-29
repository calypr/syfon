SHELL := /bin/bash
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
MKDOCS_IMAGE ?= mkdocs-material-mermaid:latest

# Generate Go server stubs from the OpenAPI spec and post\-process the bundle
.PHONY: gen
gen:
	@mkdir -p .tmp internal/apigen
	# delete previous generated code
	rm -rf internal/apigen
	# generate new Go Gin server code using OpenAPI Generator in Docker
	docker run --rm \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go-gin-server \
	  --skip-validate-spec \
	  --git-repo-id drs-server \
	  --git-user-id calypr \
	  -i /local/$(OPENAPI) \
	  -o /local/internal/apigen
	# remove non\-compliant or random examples from the generated OpenAPI bundle
	go run ./cmd/openapi-remove-examples

# Run the full Go test suite with a clean test cache
.PHONY: test
test:
	go clean -testcache
	go test -v ./...

# Run the application server locally, passing optional args via ARGS
.PHONY: serve
serve:
	go run ./cmd/server $(ARGS)

# Build the MkDocs Docker image used to serve and build documentation
.PHONY: docs-image
docs-image:
	docker build -f Dockerfile.mkdocs -t mkdocs-material-mermaid:latest .

# Serve the MkDocs documentation locally with live reload
.PHONY: docs
docs:
	docker run --rm -it \
	  -v "$(PWD):/docs" \
	  -p 8000:8000 \
	  $(MKDOCS_IMAGE) \
	  serve -a 0.0.0.0:8000

# Build the static MkDocs documentation site into the local site directory
.PHONY: docs-build
docs-build:
	docker run --rm -it \
	  -v "$(PWD):/docs" \
	  -p 8000:8000 \
	  $(MKDOCS_IMAGE) \
	  build
