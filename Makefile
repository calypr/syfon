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

gen-client:
	@mkdir -p .tmp apigen-client
	# OpenAPI Generator (Go client)
	# delete previous generated code
	rm -rf apigen-client
	# generate new code
	docker run --rm \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go \
	  --skip-validate-spec \
	  --git-repo-id drs-server \
	  --git-user-id calypr \
	  -i /local/$(OPENAPI) \
	  -o /local/apigen/drsclient \
	  --additional-properties packageName=drsclient,withGoMod=false,isGoSubmodule=true

.PHONY: test
test:
	go clean -testcache
	go test -v ./...

.PHONY: test-unit
test-unit:
	go clean -testcache
	@PKGS=$$(go list ./... | grep -Ev '/cmd/server$$|/tests/endpoints$$'); \
	  go test -v -count=1 $$PKGS

.PHONY: coverage
coverage:
	chmod +x ./scripts/run_coverage.sh
	./scripts/run_coverage.sh

.PHONY: coverage-full
coverage-full:
	chmod +x ./scripts/run_coverage.sh
	COVERAGE_SCOPE=full ./scripts/run_coverage.sh

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
