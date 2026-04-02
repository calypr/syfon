#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DRS_SERVER_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="${ENV_FILE:-$DRS_SERVER_ROOT/.env}"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
fi

TEST_DRS_URL="${TEST_DRS_URL:-${SYFON_E2E_SERVER_URL:-${DRS_URL:-http://127.0.0.1:8080}}}"

log() { printf '[syfon-e2e-hash-lookup] %s\n' "$*"; }
fail() { printf '[syfon-e2e-hash-lookup] ERROR: %s\n' "$*" >&2; exit 1; }

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
    return
  fi
  printf '%s-%s-%s-%s-%s\n' \
    "$(openssl rand -hex 4)" \
    "$(openssl rand -hex 2)" \
    "$(openssl rand -hex 2)" \
    "$(openssl rand -hex 2)" \
    "$(openssl rand -hex 6)"
}

health_check() {
  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' "${TEST_DRS_URL%/}/healthz")"
  [[ "$status" == "200" ]] || fail "health check failed: ${TEST_DRS_URL%/}/healthz returned $status"
}

post_index_record() {
  local did="$1"
  local sha="$2"
  local file_name="$3"

  local payload status
  payload="$(jq -nc \
    --arg did "$did" \
    --arg file_name "$file_name" \
    --arg sha "$sha" \
    '{did:$did,file_name:$file_name,size:1,hashes:{sha256:$sha}}')"

  status="$(curl -sS -o /tmp/syfon-hash-lookup-create.out -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data "$payload" \
    "${TEST_DRS_URL%/}/index")"

  if [[ "$status" != "201" ]]; then
    cat /tmp/syfon-hash-lookup-create.out >&2 || true
    fail "failed to create index record did=$did (status=$status)"
  fi
}

main() {
  log "using server: ${TEST_DRS_URL%/}"
  health_check

  local canonical_did alias_did fixture sha status query_file record_count first_did has_alias
  canonical_did="$(new_uuid)"
  alias_did="$(new_uuid)"
  fixture="syfon-hash-lookup-$(date +%s)-$RANDOM"
  sha="$(printf '%s' "$fixture" | sha256sum | awk '{print $1}')"

  trap 'rm -f /tmp/syfon-hash-lookup-create.out /tmp/syfon-hash-lookup-query.out' EXIT

  log "create canonical record did=$canonical_did"
  post_index_record "$canonical_did" "$sha" "hash-lookup-canonical.txt"

  log "create alias record did=$alias_did (same sha256)"
  post_index_record "$alias_did" "$sha" "hash-lookup-alias.txt"

  log "lookup by checksum"
  query_file="/tmp/syfon-hash-lookup-query.out"
  status="$(curl -sS -o "$query_file" -w '%{http_code}' \
    "${TEST_DRS_URL%/}/index?hash=sha256:${sha}")"
  [[ "$status" == "200" ]] || fail "hash lookup failed (status=$status)"

  record_count="$(jq '.records | length' "$query_file")"
  first_did="$(jq -r '.records[0].did // ""' "$query_file")"
  has_alias="$(jq --arg alias "$alias_did" '[.records[] | select(.did == $alias)] | length' "$query_file")"

  [[ "$record_count" == "1" ]] || fail "expected exactly 1 record for checksum lookup, got=$record_count"
  [[ "$first_did" == "$canonical_did" ]] || fail "expected canonical did=$canonical_did, got did=$first_did"
  [[ "$has_alias" == "0" ]] || fail "expected checksum lookup to exclude alias did=$alias_did"

  log "PASS sha256=$sha canonical_did=$canonical_did alias_did=$alias_did records=$record_count"
}

main "$@"
