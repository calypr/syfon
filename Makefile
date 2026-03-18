SHELL := /bin/bash
.DEFAULT_GOAL := build
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
MKDOCS_IMAGE ?= squidfunk/mkdocs-material:latest
GEN_OUT ?= .tmp/apigen.gen
SCHEMAS_SUBMODULE ?= ga4gh/data-repository-service-schemas
AUTO_INIT_SUBMODULE ?= 0
GOCACHE ?= $(PWD)/.gocache

.PHONY: init-schemas
init-schemas:
	@git submodule update --init --recursive --depth 1 "$(SCHEMAS_SUBMODULE)"

.PHONY: build
build:
	GOCACHE="$(GOCACHE)" go build ./...

.PHONY: gen
gen:
	@set -euo pipefail; \
	mkdir -p .tmp; \
	spec="$(OPENAPI)"; \
	if [[ ! -f "$$spec" ]]; then \
	  if [[ "$(AUTO_INIT_SUBMODULE)" == "1" ]]; then \
	    echo "OpenAPI spec '$$spec' not found. Initializing submodule..."; \
	    git submodule update --init --recursive --depth 1 "$(SCHEMAS_SUBMODULE)" || true; \
	  fi; \
	fi; \
	if [[ ! -f "$$spec" ]]; then \
	  echo "ERROR: OpenAPI spec '$$spec' not found."; \
	  echo "Run: make init-schemas"; \
	  echo "Or: make gen AUTO_INIT_SUBMODULE=1"; \
	  exit 1; \
	fi; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "ERROR: docker is required for 'make gen'."; \
	  exit 1; \
	fi; \
	rm -rf "$(GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go-server \
	  --skip-validate-spec \
	  --git-repo-id drs-server \
	  --git-user-id calypr \
	  -i /local/$$spec \
	  -o /local/$(GEN_OUT) \
	  --additional-properties outputAsLibrary=true,sourceFolder=drs,packageName=drs; \
	if [[ ! -f "$(GEN_OUT)/drs/api.go" ]]; then \
	  echo "ERROR: generation did not produce expected file: $(GEN_OUT)/drs/api.go"; \
	  exit 1; \
	fi; \
	rm -rf .tmp/apigen.prev; \
	if [[ -d apigen ]]; then mv apigen .tmp/apigen.prev; fi; \
	mv "$(GEN_OUT)" apigen; \
	echo "Generated OpenAPI server stubs into ./apigen"

.PHONY: test
test:
	GOCACHE="$(GOCACHE)" go clean -testcache
	GOCACHE="$(GOCACHE)" go test -v ./...

.PHONY: test-unit
test-unit:
	GOCACHE="$(GOCACHE)" go clean -testcache
	@PKGS=$$(go list ./... | grep -Ev '/cmd/server$$|/tests/endpoints$$'); \
	  GOCACHE="$(GOCACHE)" go test -v -count=1 $$PKGS

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
