SHELL := /bin/bash
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
MKDOCS_IMAGE ?= squidfunk/mkdocs-material:latest

.PHONY: gen
gen:
	@mkdir -p .tmp apigen
	# OpenAPI Generator (Go server stub)
	# delete previous generated code
	rm -rf apigen
	# generate new code
	docker run --rm \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go-server \
	  --skip-validate-spec \
	  --git-repo-id drs-server \
	  --git-user-id calypr \
	  -i /local/$(OPENAPI) \
	  -o /local/apigen \
	  --additional-properties outputAsLibrary=true,sourceFolder=drs,packageName=drs
	# a bundle is created at apigen/openapi.yaml, remove examples from it
	# as many are not compliant with the spec or seem to be randomly generated
	# go run ./cmd/openapi-remove-examples

.PHONY: test
test:
	go clean -testcache
	go test -v ./...

.PHONY: serve
serve:
	go run ./cmd/server $(ARGS)

.PHONY: docs
docs:
	docker run --rm -it \
	  -v "$(PWD):/docs" \
	  --user "$$(id -u):$$(id -g)" \
	  -p 8000:8000 \
	  $(MKDOCS_IMAGE) \
	  serve -a 0.0.0.0:8000