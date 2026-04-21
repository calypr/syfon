#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/coverage"
OUT_FILE="${OUT_DIR}/coverage.out"
HTML_FILE="${OUT_DIR}/coverage.html"
SCOPE="${COVERAGE_SCOPE:-meaningful}"

mkdir -p "${OUT_DIR}"

case "${SCOPE}" in
  full)
    PKGS="$(cd "${ROOT_DIR}" && go list ./...)"
    ;;
  meaningful)
<<<<<<< Updated upstream
    PKGS="$(cd "${ROOT_DIR}" && go list ./... | grep -Ev '^github.com/calypr/syfon$|/apigen/|/tests/endpoints$|/testutils$|/cmd$|/cmd/openapi-remove-examples$|/db$')"
=======
    # Exclude generated code, test helpers, and all cmd/* subpackages — the latter
    # are ignored by codecov.yml (/cmd/) so including them skews coverage numbers.
    PKGS="$(cd "${ROOT_DIR}" && go list ./... | grep -Ev '^github.com/calypr/syfon$|/apigen/|/tests/endpoints|/testutils$|/cmd/')"
>>>>>>> Stashed changes
    ;;
  core)
    PKGS="github.com/calypr/syfon/db/core github.com/calypr/syfon/db/sqlite github.com/calypr/syfon/internal/api/middleware github.com/calypr/syfon/service github.com/calypr/syfon/urlmanager"
    ;;
  *)
    echo "unknown COVERAGE_SCOPE='${SCOPE}'. valid: full, meaningful, core"
    exit 1
    ;;
esac

if [[ -z "${PKGS}" ]]; then
  echo "no packages selected for coverage run"
  exit 1
fi

<<<<<<< Updated upstream
cd "${ROOT_DIR}"
go test -count=1 -covermode=atomic -coverprofile "${OUT_FILE}" ${PKGS}
=======
cd "${WORK_DIR}"
CGO_ENABLED=1 go test -count=1 -covermode=atomic -coverprofile "${OUT_FILE}" ${PKGS}
>>>>>>> Stashed changes
go tool cover -func="${OUT_FILE}" | tee "${OUT_DIR}/coverage.txt"
go tool cover -html="${OUT_FILE}" -o "${HTML_FILE}"

echo "coverage scope:    ${SCOPE}"
echo "coverage profile: ${OUT_FILE}"
echo "coverage report:  ${OUT_DIR}/coverage.txt"
echo "coverage html:    ${HTML_FILE}"
