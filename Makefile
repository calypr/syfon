SHELL := /bin/bash
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
MKDOCS_IMAGE ?= mkdocs-material-mermaid:latest


.PHONY: gen
gen:
	@mkdir -p .tmp internal/apigen
	# OpenAPI Generator (Go server stub)
	# delete previous generated code
	rm -rf internal/apigen
	# generate new code
	docker run --rm \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go-gin-server \
	  --skip-validate-spec \
	  --git-repo-id drs-server \
	  --git-user-id calypr \
	  -i /local/$(OPENAPI) \
	  -o /local/internal/apigen
	# a bundle is created at internal/apigen/openapi.yaml, remove examples from it
	# as many are not compliant with the spec or seem to be randomly generated
	go run ./cmd/openapi-remove-examples

.PHONY: test
test:
	go clean -testcache
	go test -v ./...

.PHONY: serve
serve:
	go run ./cmd/server $(ARGS)


.PHONY: docs-image
docs-image:
	docker build -f Dockerfile.mkdocs -t mkdocs-material-mermaid:latest .

.PHONY: docs
docs:
	docker run --rm -it \
	  -v "$(PWD):/docs" \
	  -p 8000:8000 \
	  $(MKDOCS_IMAGE) \
	  serve -a 0.0.0.0:8000

.PHONY: docs-build
docs-build:
	docker run --rm -it \
	  -v "$(PWD):/docs" \
	  -p 8000:8000 \
	  $(MKDOCS_IMAGE) \
	  build