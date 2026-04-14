SHELL := /bin/bash
.DEFAULT_GOAL := build
OPENAPI ?= ga4gh/data-repository-service-schemas/openapi/data_repository_service.openapi.yaml
OAG_IMAGE ?= openapitools/openapi-generator-cli:latest
REDOCLY_IMAGE ?= redocly/cli:latest
YQ_IMAGE ?= mikefarah/yq:latest
MKDOCS_IMAGE ?= squidfunk/mkdocs-material:latest
GEN_OUT ?= .tmp/apigen.gen
LFS_OPENAPI ?= apigen/api/lfs.openapi.yaml
LFS_GEN_OUT ?= .tmp/apigen-lfs.gen
BUCKET_OPENAPI ?= apigen/api/bucket.openapi.yaml
BUCKET_GEN_OUT ?= .tmp/apigen-bucket.gen
METRICS_OPENAPI ?= apigen/api/metrics.openapi.yaml
METRICS_GEN_OUT ?= .tmp/apigen-metrics.gen
INTERNAL_OPENAPI ?= apigen/api/internal.openapi.yaml
INTERNAL_GEN_OUT ?= .tmp/apigen-internal.gen
SCHEMAS_SUBMODULE ?= ga4gh/data-repository-service-schemas
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
	@GOCACHE="$(GOCACHE)" go build -ldflags "$(LDFLAGS)" -o syfon ./...

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
	rm -rf "$(GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go-server \
	  --skip-validate-spec \
	  --git-repo-id syfon \
	  --git-user-id calypr \
	  -i /local/apigen/api/openapi.yaml \
	  -o /local/$(GEN_OUT) \
	  --additional-properties outputAsLibrary=true,sourceFolder=drs,packageName=drs; \
	if [[ ! -f "$(GEN_OUT)/drs/api.go" ]]; then \
	  echo "ERROR: generation did not produce expected file: $(GEN_OUT)/drs/api.go"; \
	  exit 1; \
	fi; \
	mkdir -p apigen/api apigen; \
	rm -rf apigen/drs; \
	cp -R "$(GEN_OUT)/drs" apigen/drs; \
	cp -f "$(GEN_OUT)/README.md" apigen/README.md; \
	cp -f "$(GEN_OUT)/.openapi-generator-ignore" apigen/.openapi-generator-ignore; \
	rm -rf apigen/.openapi-generator; \
	cp -R "$(GEN_OUT)/.openapi-generator" apigen/.openapi-generator; \
	echo "Generated OpenAPI server stubs into ./apigen/drs and bundled spec into ./apigen/api/openapi.yaml"; \
	if [[ -f "$(LFS_OPENAPI)" ]]; then \
	  $(MAKE) gen-lfs; \
	else \
	  if [[ -d apigen/lfsapi ]] && ls apigen/lfsapi/*.go >/dev/null 2>&1; then \
	    echo "WARNING: $(LFS_OPENAPI) not found; preserving existing apigen/lfsapi."; \
	  else \
	    echo "ERROR: $(LFS_OPENAPI) is missing and apigen/lfsapi does not exist."; \
	    echo "Conflict: LFS model generation input is missing."; \
	    exit 1; \
	  fi; \
	fi
	@if [[ -f "$(BUCKET_OPENAPI)" ]]; then \
	  $(MAKE) gen-bucket; \
	else \
	  if [[ -d apigen/bucketapi ]] && ls apigen/bucketapi/*.go >/dev/null 2>&1; then \
	    echo "WARNING: $(BUCKET_OPENAPI) not found; preserving existing apigen/bucketapi."; \
	  else \
	    echo "WARNING: $(BUCKET_OPENAPI) is missing; bucket models will not be generated."; \
	  fi; \
	fi
	@if [[ -f "$(METRICS_OPENAPI)" ]]; then \
	  $(MAKE) gen-metrics; \
	else \
	  if [[ -d apigen/metricsapi ]] && ls apigen/metricsapi/*.go >/dev/null 2>&1; then \
	    echo "WARNING: $(METRICS_OPENAPI) not found; preserving existing apigen/metricsapi."; \
	  else \
	    echo "WARNING: $(METRICS_OPENAPI) is missing; metrics models will not be generated."; \
	  fi; \
	fi
	@if [[ -f "$(INTERNAL_OPENAPI)" ]]; then \
	  $(MAKE) gen-internal; \
	else \
	  if [[ -d apigen/internalapi ]] && ls apigen/internalapi/*.go >/dev/null 2>&1; then \
	    echo "WARNING: $(INTERNAL_OPENAPI) not found; preserving existing apigen/internalapi."; \
	  else \
	    echo "WARNING: $(INTERNAL_OPENAPI) is missing; internal models will not be generated."; \
	  fi; \
	fi

.PHONY: gen-lfs
gen-lfs:
	@set -euo pipefail; \
	if [[ ! -f "$(LFS_OPENAPI)" ]]; then \
	  echo "ERROR: LFS OpenAPI spec '$(LFS_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "ERROR: docker is required for 'make gen-lfs'."; \
	  exit 1; \
	fi; \
	echo "Validating/Bundling LFS spec with Redocly..."; \
	docker run --rm -v "$(PWD):/spec" $(REDOCLY_IMAGE) bundle /spec/$(LFS_OPENAPI) --output /spec/apigen/api/lfs.openapi.yaml --ext yaml; \
	rm -rf "$(LFS_GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go \
	  --skip-validate-spec \
	  --git-repo-id syfon \
	  --git-user-id calypr \
	  -i /local/apigen/api/lfs.openapi.yaml \
	  -o /local/$(LFS_GEN_OUT) \
	  --global-property models,modelDocs=false,modelTests=false,supportingFiles=utils.go \
	  --additional-properties packageName=lfsapi,enumClassPrefix=true; \
	if [[ ! -d "$(LFS_GEN_OUT)" ]]; then \
	  echo "ERROR: generation did not produce expected dir: $(LFS_GEN_OUT)"; \
	  exit 1; \
	fi; \
	rm -rf apigen/lfsapi; \
	mkdir -p apigen/lfsapi; \
	find "$(LFS_GEN_OUT)" -maxdepth 1 -type f -name '*.go' -exec mv {} apigen/lfsapi/ \; ; \
	echo "Generated LFS OpenAPI models into ./apigen/lfsapi"

.PHONY: gen-bucket
gen-bucket:
	@set -euo pipefail; \
	if [[ ! -f "$(BUCKET_OPENAPI)" ]]; then \
	  echo "ERROR: Bucket OpenAPI spec '$(BUCKET_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "ERROR: docker is required for 'make gen-bucket'."; \
	  exit 1; \
	fi; \
	echo "Validating/Bundling Bucket spec with Redocly..."; \
	docker run --rm -v "$(PWD):/spec" $(REDOCLY_IMAGE) bundle /spec/$(BUCKET_OPENAPI) --output /spec/apigen/api/bucket.openapi.yaml --ext yaml; \
	rm -rf "$(BUCKET_GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go \
	  --skip-validate-spec \
	  --git-repo-id syfon \
	  --git-user-id calypr \
	  -i /local/apigen/api/bucket.openapi.yaml \
	  -o /local/$(BUCKET_GEN_OUT) \
	  --global-property models,modelDocs=false,modelTests=false,supportingFiles=utils.go \
	  --additional-properties packageName=bucketapi,enumClassPrefix=true; \
	if [[ ! -d "$(BUCKET_GEN_OUT)" ]]; then \
	  echo "ERROR: generation did not produce expected dir: $(BUCKET_GEN_OUT)"; \
	  exit 1; \
	fi; \
	rm -rf apigen/bucketapi; \
	mkdir -p apigen/bucketapi; \
	find "$(BUCKET_GEN_OUT)" -maxdepth 1 -type f -name '*.go' -exec mv {} apigen/bucketapi/ \; ; \
	echo "Generated Bucket OpenAPI models into ./apigen/bucketapi"

.PHONY: gen-metrics
gen-metrics:
	@set -euo pipefail; \
	if [[ ! -f "$(METRICS_OPENAPI)" ]]; then \
	  echo "ERROR: Metrics OpenAPI spec '$(METRICS_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "ERROR: docker is required for 'make gen-metrics'."; \
	  exit 1; \
	fi; \
	echo "Validating/Bundling Metrics spec with Redocly..."; \
	docker run --rm -v "$(PWD):/spec" $(REDOCLY_IMAGE) bundle /spec/$(METRICS_OPENAPI) --output /spec/apigen/api/metrics.openapi.yaml --ext yaml; \
	rm -rf "$(METRICS_GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go \
	  --skip-validate-spec \
	  --git-repo-id syfon \
	  --git-user-id calypr \
	  -i /local/apigen/api/metrics.openapi.yaml \
	  -o /local/$(METRICS_GEN_OUT) \
	  --global-property models,modelDocs=false,modelTests=false,supportingFiles=utils.go \
	  --additional-properties packageName=metricsapi,enumClassPrefix=true; \
	if [[ ! -d "$(METRICS_GEN_OUT)" ]]; then \
	  echo "ERROR: generation did not produce expected dir: $(METRICS_GEN_OUT)"; \
	  exit 1; \
	fi; \
	rm -rf apigen/metricsapi; \
	mkdir -p apigen/metricsapi; \
	find "$(METRICS_GEN_OUT)" -maxdepth 1 -type f -name '*.go' -exec mv {} apigen/metricsapi/ \; ; \
	echo "Generated Metrics OpenAPI models into ./apigen/metricsapi"

.PHONY: gen-internal
gen-internal:
	@set -euo pipefail; \
	if [[ ! -f "$(INTERNAL_OPENAPI)" ]]; then \
	  echo "ERROR: Internal OpenAPI spec '$(INTERNAL_OPENAPI)' not found."; \
	  exit 1; \
	fi; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "ERROR: docker is required for 'make gen-internal'."; \
	  exit 1; \
	fi; \
	echo "Validating/Bundling Internal spec with Redocly..."; \
	docker run --rm -v "$(PWD):/spec" $(REDOCLY_IMAGE) bundle /spec/$(INTERNAL_OPENAPI) --output /spec/apigen/api/internal.openapi.yaml --ext yaml; \
	rm -rf "$(INTERNAL_GEN_OUT)"; \
	docker run --rm --pull=missing \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$(PWD):/local" \
	  $(OAG_IMAGE) generate \
	  -g go \
	  --skip-validate-spec \
	  --git-repo-id syfon \
	  --git-user-id calypr \
	  -i /local/apigen/api/internal.openapi.yaml \
	  -o /local/$(INTERNAL_GEN_OUT) \
	  --global-property models,modelDocs=false,modelTests=false,supportingFiles=utils.go \
	  --additional-properties packageName=internalapi,enumClassPrefix=true; \
	if [[ ! -d "$(INTERNAL_GEN_OUT)" ]]; then \
	  echo "ERROR: generation did not produce expected dir: $(INTERNAL_GEN_OUT)"; \
	  exit 1; \
	fi; \
	rm -rf apigen/internalapi; \
	mkdir -p apigen/internalapi; \
	find "$(INTERNAL_GEN_OUT)" -maxdepth 1 -type f -name '*.go' -exec mv {} apigen/internalapi/ \; ; \
	echo "Generated Internal OpenAPI models into ./apigen/internalapi"

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
