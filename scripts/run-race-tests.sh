#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="${GOCACHE:-${TMPDIR:-/tmp}/syfon-race-cache}"

cd "$repo_root"
CGO_ENABLED=1 GOCACHE="$cache_dir" go test -race -count=1 \
  ./internal/httpapi \
  ./internal/transfers \
  ./internal/transfers/lfs \
  ./internal/projects/storage \
  ./internal/access/authentication

cd "$repo_root/client"
client_mod_dir="$(mktemp -d "${TMPDIR:-/tmp}/syfon-race-client.XXXXXX")"
trap 'rm -rf "$client_mod_dir"' EXIT
client_modfile="$client_mod_dir/client.mod"
cp go.mod "$client_modfile"
cp go.sum "$client_mod_dir/client.sum"
go mod edit -modfile="$client_modfile" -replace="github.com/calypr/syfon/apigen=$repo_root/apigen"
GOWORK=off CGO_ENABLED=1 GOCACHE="$cache_dir" GOFLAGS="-modfile=$client_modfile" go test -race -count=1 ./...
