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
MODEL_OPENAPI ?= apigen/model/openapi.yaml
MODEL_CONFIG ?= apigen/specs/oapi-model.yaml
SCHEMAS_SUBMODULE ?= ga4gh/data-repository-service-schemas
OAPI_DRS_GIN_CONFIG ?= apigen/specs/oapi-drs.yaml
OAPI_LFS_CONFIG ?= apigen/specs/oapi-lfs.yaml
OAPI_BUCKET_CONFIG ?= apigen/specs/oapi-bucket.yaml
OAPI_METRICS_CONFIG ?= apigen/specs/oapi-metrics.yaml
OAPI_INTERNAL_CONFIG ?= apigen/specs/oapi-internal.yaml
CLIENT_OAPI_DRS_CONFIG ?= apigen/specs/client-oapi-drs.yaml
CLIENT_OAPI_LFS_CONFIG ?= apigen/specs/client-oapi-lfs.yaml
CLIENT_OAPI_BUCKET_CONFIG ?= apigen/specs/client-oapi-bucket.yaml
CLIENT_OAPI_METRICS_CONFIG ?= apigen/specs/client-oapi-metrics.yaml
CLIENT_OAPI_INTERNAL_CONFIG ?= apigen/specs/client-oapi-internal.yaml

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
LDFLAGS     := -X github.com/calypr/syfon/internal/version.Version=$(GIT_VERSION) \
               -X github.com/calypr/syfon/internal/version.GitCommit=$(GIT_COMMIT) \
               -X github.com/calypr/syfon/internal/version.GitBranch=$(GIT_BRANCH) \
               -X github.com/calypr/syfon/internal/version.BuildDate=$(BUILD_DATE) \
               -X github.com/calypr/syfon/internal/version.GitUpstream=$(GIT_UPSTREAM)

.PHONY: build
build:
	CGO_ENABLED=1 GOCACHE="$(GOCACHE)" go build -ldflags "$(LDFLAGS)" -o syfon .

.PHONY: install
install:
	@GOCACHE="$(GOCACHE)" go install -ldflags "$(LDFLAGS)" ./...

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
	if [[ ! -f "$(MODEL_CONFIG)" ]]; then \
	  echo "ERROR: oapi-codegen config '$(MODEL_CONFIG)' not found."; \
	  exit 1; \
	fi; \
	$(MAKE) gen-model; \
	$(MAKE) gen-server; \
	$(MAKE) gen-client

.PHONY: gen-model
gen-model:
	@set -euo pipefail; \
	mkdir -p apigen/model; \
	cp apigen/api/openapi.yaml "$(MODEL_OPENAPI)"; \
	echo "Generating shared model bindings with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(MODEL_CONFIG)" "$(MODEL_OPENAPI)" > apigen/model/model.gen.go; \
	echo "Generated shared model bindings into ./apigen/model/model.gen.go"

.PHONY: gen-server
gen-server:
	@set -euo pipefail; \
	mkdir -p apigen/server/drs apigen/server/lfsapi apigen/server/bucketapi apigen/server/metricsapi apigen/server/internalapi; \
	echo "Generating Fiber v3 strict server bindings with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_DRS_GIN_CONFIG)" apigen/api/openapi.yaml > apigen/server/drs/drs.gen.go; \
	echo "Generated Fiber v3 strict server bindings into ./apigen/server/drs/drs.gen.go"; \
	echo "Generating LFS Fiber v3 strict server with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_LFS_CONFIG)" apigen/api/lfs.openapi.yaml > apigen/server/lfsapi/lfs.gen.go; \
	echo "Generated LFS Fiber v3 strict server into ./apigen/server/lfsapi/lfs.gen.go"; \
	echo "Generating Bucket Fiber v3 strict server with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_BUCKET_CONFIG)" apigen/api/bucket.openapi.yaml > apigen/server/bucketapi/bucket.gen.go; \
	echo "Generated Bucket Fiber v3 strict server into ./apigen/server/bucketapi/bucket.gen.go"; \
	echo "Generating Metrics Fiber v3 strict server with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_METRICS_CONFIG)" apigen/api/metrics.openapi.yaml > apigen/server/metricsapi/metrics.gen.go; \
	echo "Generated Metrics Fiber v3 strict server into ./apigen/server/metricsapi/metrics.gen.go"; \
	echo "Generating Internal Fiber v3 strict server with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(OAPI_INTERNAL_CONFIG)" apigen/api/internal.openapi.yaml > apigen/server/internalapi/internal.gen.go; \
	echo "Generated Internal Fiber v3 strict server into ./apigen/server/internalapi/internal.gen.go"

.PHONY: gen-client
gen-client:
	@set -euo pipefail; \
	mkdir -p apigen/client/drs apigen/client/lfsapi apigen/client/bucketapi apigen/client/metricsapi apigen/client/internalapi; \
	if [[ ! -f "$(CLIENT_OAPI_DRS_CONFIG)" ]]; then \
	  echo "ERROR: client oapi-codegen config '$(CLIENT_OAPI_DRS_CONFIG)' not found."; \
	  exit 1; \
	fi; \
	echo "Generating separate client bindings with oapi-codegen..."; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(CLIENT_OAPI_DRS_CONFIG)" apigen/api/openapi.yaml > apigen/client/drs/drs.gen.go; \
	echo "Generated DRS client bindings into ./apigen/client/drs/drs.gen.go"; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(CLIENT_OAPI_LFS_CONFIG)" apigen/api/lfs.openapi.yaml > apigen/client/lfsapi/lfs.gen.go; \
	echo "Generated LFS client bindings into ./apigen/client/lfsapi/lfs.gen.go"; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(CLIENT_OAPI_BUCKET_CONFIG)" apigen/api/bucket.openapi.yaml > apigen/client/bucketapi/bucket.gen.go; \
	echo "Generated Bucket client bindings into ./apigen/client/bucketapi/bucket.gen.go"; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(CLIENT_OAPI_METRICS_CONFIG)" apigen/api/metrics.openapi.yaml > apigen/client/metricsapi/metrics.gen.go; \
	echo "Generated Metrics client bindings into ./apigen/client/metricsapi/metrics.gen.go"; \
	GOTOOLCHAIN=local $(OAPI_CODEGEN) -config "$(CLIENT_OAPI_INTERNAL_CONFIG)" apigen/api/internal.openapi.yaml > apigen/client/internalapi/internal.gen.go; \
	echo "Generated Internal client bindings into ./apigen/client/internalapi/internal.gen.go"; \
	echo "Generated client bindings into ./apigen/client/*"

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
	go run . serve $(ARGS)

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

.PHONY: build-local-auth-plugin
build-local-auth-plugin:
	cd plugins/local_auth && go build -o ../../bin/local_auth_plugin .

.PHONY: build-gen3-auth-plugin
build-gen3-auth-plugin:
	cd plugins/gen3_auth && go build -o ../../bin/gen3_auth_plugin .

.PHONY: build-plugins
build-plugins: build-local-auth-plugin build-gen3-auth-plugin
