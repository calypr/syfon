#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/coverage"
OUT_FILE="${OUT_DIR}/coverage.out"
HTML_FILE="${OUT_DIR}/coverage.html"
SCOPE="${COVERAGE_SCOPE:-core}"

mkdir -p "${OUT_DIR}"

if [[ "${SCOPE}" == "full" ]]; then
  PKGS="$(cd "${ROOT_DIR}" && go list ./... | grep -Ev '^github.com/calypr/drs-server$|/apigen/|/cmd$|/cmd/|/tests/endpoints$|/db$|/db/postgres$|/testutils$')"
else
  PKGS="github.com/calypr/drs-server/db/core github.com/calypr/drs-server/db/sqlite github.com/calypr/drs-server/internal/api/middleware github.com/calypr/drs-server/service github.com/calypr/drs-server/urlmanager"
fi

if [[ -z "${PKGS}" ]]; then
  echo "no packages selected for coverage run"
  exit 1
fi

cd "${ROOT_DIR}"
go test -count=1 -covermode=atomic -coverprofile "${OUT_FILE}" ${PKGS}
go tool cover -func="${OUT_FILE}" | tee "${OUT_DIR}/coverage.txt"
go tool cover -html="${OUT_FILE}" -o "${HTML_FILE}"

echo "coverage scope:    ${SCOPE}"
echo "coverage profile: ${OUT_FILE}"
echo "coverage report:  ${OUT_DIR}/coverage.txt"
echo "coverage html:    ${HTML_FILE}"
