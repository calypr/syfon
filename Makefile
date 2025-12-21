SHELL := /bin/bash
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OPENAPI_NORM ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.normalized.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
OPENAPI_BUNDLE ?= ga4gh/data-repository-service-schemas/openapi/openapi.bundled.yaml

.PHONY: tools
tools:
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.1
	go mod tidy


.PHONY: gen-oag
gen-oag:
	@mkdir -p .tmp internal/apigen
	# OpenAPI Generator (Go server stub)
	#  Skips the default behavior of validating an input specification.
	#  Skips overwriting existing files that were previously generated.

	rm -f internal/apigen/main.go
	docker run --rm \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
		-g go-gin-server \
		--skip-validate-spec \
		--skip-overwrite \
		--git-repo-id drs-server \
		--git-user-id calypr \
		-i /local/$(OPENAPI_BUNDLE) \
		-o /local/internal/apigen \
		-c /local/openapi/openapi-generator.yaml

.PHONY: gen
gen:
	@mkdir -p .tmp internal/apigen

	# Normalize OpenAPI spec
	go run ./cmd/openapi-normalize -in $(OPENAPI) -out $(OPENAPI_NORM)
	# Bundle OpenAPI spec
	#go run ./cmd/openapi-bundle -in $(OPENAPI_NORM) -out $(OPENAPI_BUNDLE)

	oapi-codegen -package apigen -generate types,chi-server,spec -o internal/apigen/openapi.gen.go $(OPENAPI_NORM)

	go run ./cmd/openapi-testgen \
	  -in $(OPENAPI_NORM) \
	  -out internal/apigen/openapi_generated_test.go \
	  -pkg apigen

.PHONY: fetch
fetch:
	@if [ -z "$(OPENAPI_URL)" ]; then echo "Set OPENAPI_URL=..."; exit 2; fi
	go run ./cmd/openapi-fetchgen -url "$(OPENAPI_URL)" -out "$(OPENAPI)" -with-refs
	$(MAKE) gen OPENAPI="$(OPENAPI)"

.PHONY: test
test:
	go test ./...
