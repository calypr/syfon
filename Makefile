SHELL := /bin/bash
.DEFAULT_GOAL := build
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
OAPI_CODEGEN ?= go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0
REDOCLY_IMAGE ?= redocly/cli:latest
YQ_IMAGE ?= mikefarah/yq:latest
MKDOCS_IMAGE ?= squidfunk/mkdocs-material:latest
LFS_OPENAPI ?= apigen/api/lfs.openapi.yaml
LFS_GEN_OUT ?= .tmp/apigen-lfs.gen
BUCKET_OPENAPI ?= apigen/api/bucket.openapi.yaml
BUCKET_GEN_OUT ?= .tmp/apigen-bucket.gen
METRICS_OPENAPI ?= apigen/api/metrics.openapi.yaml
METRICS_GEN_OUT ?= .tmp/apigen-metrics.gen
INTERNAL_OPENAPI ?= apigen/api/internal.openapi.yaml
INTERNAL_GEN_OUT ?= .tmp/apigen-internal.gen
SCHEMAS_SUBMODULE ?= ga4gh/data-repository-service-schemas
OAPI_DRS_GIN_CONFIG ?= apigen/specs/oapi-drsgin.yaml
OAPI_LFS_CONFIG ?= apigen/specs/oapi-lfs.yaml
OAPI_BUCKET_CONFIG ?= apigen/specs/oapi-bucket.yaml
OAPI_METRICS_CONFIG ?= apigen/specs/oapi-metrics.yaml
OAPI_INTERNAL_CONFIG ?= apigen/specs/oapi-internal.yaml

AUTO_INIT_SUBMODULE ?= 0
GOCACHE ?= $(PWD)/.gocache
REMOTE ?= origin
VERSION ?=
DRY_RUN ?= 0
RUN_TESTS ?= 1
APIGEN_TAG_PREFIX ?= apigen
CLIENT_TAG_PREFIX ?= client

.PHONY: init-schemas
init-schemas:
	@git submodule update --init --recursive --depth 1 "$(SCHEMAS_SUBMODULE)"

GIT_VERSION ?= $(shell git describe --tags --always --match 'v[0-9]*' --dirty='-dirty' 2>/dev/null || echo dev)
GIT_COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_BRANCH  ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_UPSTREAM ?= $(shell git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null || echo unknown)
LDFLAGS     := -X github.com/calypr/syfon/version.Version=$(GIT_VERSION) \
               -X github.com/calypr/syfon/version.GitCommit=$(GIT_COMMIT) \
               -X github.com/calypr/syfon/version.GitBranch=$(GIT_BRANCH) \
               -X github.com/calypr/syfon/version.BuildDate=$(BUILD_DATE) \
               -X github.com/calypr/syfon/version.GitUpstream=$(GIT_UPSTREAM)

.PHONY: build
build:
	@GOCACHE="$(GOCACHE)" go build -ldflags "$(LDFLAGS)" ./...

.PHONY: install
install:
	@GOCACHE="$(GOCACHE)" go install -ldflags "$(LDFLAGS)" ./...

# Build binaries for all OS/Architectures
.PHONY: snapshot
snapshot: release-dep
	@goreleaser \
		--clean \
		--snapshot

# Create a release on Github using GoReleaser
.PHONY: release
release: release-dep
	@goreleaser --clean

# Install dependencies for release
.PHONY: release-dep
release-dep:
	@go install github.com/goreleaser/goreleaser/v2@latest

.PHONY: gen
gen:
	@set -euo pipefail; \
	mkdir -p .tmp; \
	spec="$(OPENAPI)"; \
	if [[ ! -f "$$spec" ]]; then \
		  if [[ "$(AUTO_INIT_SUBMODULE)" == "1" ]]; then \
		    echo "OpenAPI spec '$$spec' not found. Initializing submodule..."; \
		    git submodule update --init --recursive --depth 1 "$(SCHEMAS_SUBMODULE)"; \
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
	echo "Bundling canonical OpenAPI spec with Redocly..."; \
	docker run --rm \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(REDOCLY_IMAGE) bundle /local/$$spec --output /local/.tmp/drs.base.yaml --ext yaml; \
	echo "Merging internal Extensions with yq..."; \
	docker run --rm \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(YQ_IMAGE) eval-all 'select(fileIndex == 0) * select(fileIndex == 1)' /local/.tmp/drs.base.yaml /local/apigen/specs/drs-extensions-overlay.yaml > apigen/api/openapi.yaml; \
	mkdir -p apigen/api apigen; \
	echo "Bundled canonical DRS OpenAPI spec into ./apigen/api/openapi.yaml"; \
	if [[ ! -f "$(OAPI_DRS_GIN_CONFIG)" ]]; then \
	  echo "ERROR: oapi-codegen config '$(OAPI_DRS_GIN_CONFIG)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating gin strict server bindings with oapi-codegen..."; \
	mkdir -p apigen/drs; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_DRS_GIN_CONFIG)" apigen/api/openapi.yaml > apigen/drs/drs.gen.go; \
	echo "Generated gin strict server bindings into ./apigen/drs/drs.gen.go"; \
	$(MAKE) gen-lfs; \
	$(MAKE) gen-bucket; \
	$(MAKE) gen-metrics; \
	$(MAKE) gen-internal

.PHONY: gen-lfs
gen-lfs:
	@set -euo pipefail; \
	if [[ ! -f "$(LFS_OPENAPI)" ]]; then \
	  echo "ERROR: LFS OpenAPI spec '$(LFS_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating LFS gin strict server with oapi-codegen..."; \
	mkdir -p apigen/lfsapi; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_LFS_CONFIG)" apigen/api/lfs.openapi.yaml > apigen/lfsapi/lfs.gen.go; \
	echo "Generated LFS gin strict server into ./apigen/lfsapi/lfs.gen.go"

.PHONY: gen-bucket
gen-bucket:
	@set -euo pipefail; \
	if [[ ! -f "$(BUCKET_OPENAPI)" ]]; then \
	  echo "ERROR: Bucket OpenAPI spec '$(BUCKET_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating Bucket gin strict server with oapi-codegen..."; \
	mkdir -p apigen/bucketapi; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_BUCKET_CONFIG)" apigen/api/bucket.openapi.yaml > apigen/bucketapi/bucket.gen.go; \
	echo "Generated Bucket gin strict server into ./apigen/bucketapi/bucket.gen.go"

.PHONY: gen-metrics
gen-metrics:
	@set -euo pipefail; \
	if [[ ! -f "$(METRICS_OPENAPI)" ]]; then \
	  echo "ERROR: Metrics OpenAPI spec '$(METRICS_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating Metrics gin strict server with oapi-codegen..."; \
	mkdir -p apigen/metricsapi; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_METRICS_CONFIG)" apigen/api/metrics.openapi.yaml > apigen/metricsapi/metrics.gen.go; \
	echo "Generated Metrics gin strict server into ./apigen/metricsapi/metrics.gen.go"

.PHONY: gen-internal
gen-internal:
	@set -euo pipefail; \
	if [[ ! -f "$(INTERNAL_OPENAPI)" ]]; then \
	  echo "ERROR: Internal OpenAPI spec '$(INTERNAL_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating Internal gin strict server with oapi-codegen..."; \
	mkdir -p apigen/internalapi; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_INTERNAL_CONFIG)" apigen/api/internal.openapi.yaml > apigen/internalapi/internal.gen.go; \
	echo "Generated Internal gin strict server into ./apigen/internalapi/internal.gen.go"

.PHONY: test
test:
	GOCACHE="$(GOCACHE)" go clean -testcache
	CGO_ENABLED=1 GOCACHE="$(GOCACHE)" go test -v ./...

.PHONY: test-unit
test-unit:
	GOCACHE="$(GOCACHE)" go clean -testcache
	@PKGS=$$(go list ./... | grep -Ev '/cmd/server$$|/tests/endpoints$$'); \
	  CGO_ENABLED=1 GOCACHE="$(GOCACHE)" go test -v -count=1 $$PKGS

.PHONY: coverage
coverage:
	chmod +x ./scripts/run_coverage.sh
	./scripts/run_coverage.sh

.PHONY: coverage-meaningful
coverage-meaningful:
	chmod +x ./scripts/run_coverage.sh
	COVERAGE_SCOPE=meaningful ./scripts/run_coverage.sh

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

.PHONY: release-plan
release-plan:
	@set -euo pipefail; \
	if [[ -z "$(VERSION)" ]]; then \
	  echo "ERROR: VERSION is required (example: make release-apigen VERSION=v0.1.0)"; \
	  exit 1; \
	fi; \
	echo "remote:      $(REMOTE)"; \
	echo "version:     $(VERSION)"; \
	echo "apigen tag:  $(APIGEN_TAG_PREFIX)/$(VERSION)"; \
	echo "client tag:  $(CLIENT_TAG_PREFIX)/$(VERSION)"; \
	echo "dry run:     $(DRY_RUN)"

.PHONY: release-check-version
release-check-version:
	@set -euo pipefail; \
	if [[ -z "$(VERSION)" ]]; then \
	  echo "ERROR: VERSION is required (example: VERSION=v0.1.0)"; \
	  exit 1; \
	fi; \
	if [[ ! "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$$ ]]; then \
	  echo "ERROR: VERSION must look like vX.Y.Z (got: $(VERSION))"; \
	  exit 1; \
	fi

.PHONY: release-check-clean
release-check-clean:
	@set -euo pipefail; \
	if [[ -n "$$(git status --porcelain)" ]]; then \
	  if [[ "$(DRY_RUN)" == "1" ]]; then \
	    echo "WARN: git tree is dirty (dry run continuing)"; \
	    git status --short; \
	    exit 0; \
	  fi; \
	  echo "ERROR: git tree is dirty. Commit/stash changes before releasing."; \
	  git status --short; \
	  exit 1; \
	fi

.PHONY: release-check-clean-apigen
release-check-clean-apigen:
	@set -euo pipefail; \
	dirty="$$(git status --porcelain -- apigen)"; \
	if [[ -n "$$dirty" ]]; then \
	  if [[ "$(DRY_RUN)" == "1" ]]; then \
	    echo "WARN: apigen tree is dirty (dry run continuing)"; \
	    printf "%s\n" "$$dirty"; \
	    exit 0; \
	  fi; \
	  echo "ERROR: apigen tree is dirty. Commit/stash apigen changes before releasing apigen."; \
	  printf "%s\n" "$$dirty"; \
	  exit 1; \
	fi

.PHONY: release-check-clean-client
release-check-clean-client:
	@set -euo pipefail; \
	dirty="$$(git status --porcelain -- client)"; \
	if [[ -n "$$dirty" ]]; then \
	  if [[ "$(DRY_RUN)" == "1" ]]; then \
	    echo "WARN: client tree is dirty (dry run continuing)"; \
	    printf "%s\n" "$$dirty"; \
	    exit 0; \
	  fi; \
	  echo "ERROR: client tree is dirty. Commit/stash client changes before releasing client."; \
	  printf "%s\n" "$$dirty"; \
	  exit 1; \
	fi

.PHONY: release-check-apigen-tag
release-check-apigen-tag: release-check-version
	@set -euo pipefail; \
	tag="$(APIGEN_TAG_PREFIX)/$(VERSION)"; \
	if git rev-parse "$$tag" >/dev/null 2>&1; then \
	  if [[ "$(DRY_RUN)" == "1" ]]; then \
	    echo "WARN: tag already exists locally (dry run continuing): $$tag"; \
	    exit 0; \
	  fi; \
	  echo "ERROR: tag already exists locally: $$tag"; \
	  exit 1; \
	fi

.PHONY: release-check-client-tag
release-check-client-tag: release-check-version
	@set -euo pipefail; \
	tag="$(CLIENT_TAG_PREFIX)/$(VERSION)"; \
	if git rev-parse "$$tag" >/dev/null 2>&1; then \
	  if [[ "$(DRY_RUN)" == "1" ]]; then \
	    echo "WARN: tag already exists locally (dry run continuing): $$tag"; \
	    exit 0; \
	  fi; \
	  echo "ERROR: tag already exists locally: $$tag"; \
	  exit 1; \
	fi

.PHONY: release-test-apigen
release-test-apigen:
	@set -euo pipefail; \
	if [[ "$(RUN_TESTS)" != "1" ]]; then \
	  echo "Skipping apigen tests (RUN_TESTS=$(RUN_TESTS))"; \
	  exit 0; \
	fi; \
	cd apigen; \
	CGO_ENABLED=1 GOCACHE="$(GOCACHE)" go test ./...

.PHONY: release-test-client
release-test-client:
	@set -euo pipefail; \
	if [[ "$(RUN_TESTS)" != "1" ]]; then \
	  echo "Skipping client tests (RUN_TESTS=$(RUN_TESTS))"; \
	  exit 0; \
	fi; \
	cd client; \
	CGO_ENABLED=1 GOCACHE="$(GOCACHE)" go test ./...

.PHONY: release-apigen
release-apigen: release-check-clean-apigen release-check-apigen-tag release-test-apigen
	@set -euo pipefail; \
	tag="$(APIGEN_TAG_PREFIX)/$(VERSION)"; \
	if [[ "$(DRY_RUN)" == "1" ]]; then \
	  echo "[DRY RUN] git tag -a $$tag -m \"Release $$tag\""; \
	  echo "[DRY RUN] git push $(REMOTE) $$tag"; \
	  exit 0; \
	fi; \
	git tag -a "$$tag" -m "Release $$tag"; \
	git push "$(REMOTE)" "$$tag"; \
	echo "Released $$tag"

.PHONY: release-client
release-client: release-check-clean-client release-check-client-tag release-test-client
	@set -euo pipefail; \
	tag="$(CLIENT_TAG_PREFIX)/$(VERSION)"; \
	if [[ "$(DRY_RUN)" == "1" ]]; then \
	  echo "[DRY RUN] git tag -a $$tag -m \"Release $$tag\""; \
	  echo "[DRY RUN] git push $(REMOTE) $$tag"; \
	  exit 0; \
	fi; \
	git tag -a "$$tag" -m "Release $$tag"; \
	git push "$(REMOTE)" "$$tag"; \
	echo "Released $$tag"
