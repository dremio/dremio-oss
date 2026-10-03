#!/usr/bin/env bash
#
# Copyright (C) 2017-2019 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# End-to-end smoke test of the harness for one INSTANCE:
#   Polaris up -> Dremio up -> RESTCATALOG source (state good) -> namespace + table created via
#   Polaris API are visible in Dremio -> CREATE TABLE / INSERT / SELECT via SQL -> secret scan
#   -> teardown (source, Dremio, Polaris, S3 prefix).
#
# Usage: INSTANCE=0 S3_ACCESS_KEY=... S3_SECRET_KEY=... smoke.sh
# Env: SMOKE_KEEP=1 keeps the stack running (no teardown), SMOKE_NS (default smoke),
#      SOURCE_NAME (default polaris), SOURCE_NAMES_REFRESH_MS (default 60000 here)
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_s3_creds
S="$E2E_DIR"
NS="${SMOKE_NS:-smoke}"
SRC="${SOURCE_NAME:-polaris}"
failures=0

step() { printf '\n== %s\n' "$*"; }
check() { # check DESCRIPTION CMD... (records failure, keeps going)
  local what="$1"
  shift
  if "$@"; then echo "PASS  $what"; else echo "FAIL  $what"; failures=$((failures + 1)); fi
}

teardown() {
  step "teardown"
  "$S/source.sh" delete "$SRC" || true
  "$S/dremio-down.sh" --purge || true
  "$S/polaris-down.sh" || true
  "$S/s3.sh" clean || true
}
if [ "${SMOKE_KEEP:-0}" != 1 ]; then trap teardown EXIT; fi

step "polaris-up"
"$S/polaris-up.sh"
step "dremio-up"
"$S/dremio-up.sh"

step "source create"
out="$(SOURCE_NAMES_REFRESH_MS="${SOURCE_NAMES_REFRESH_MS:-60000}" "$S/source.sh" create "$SRC" || true)"
echo "$out"
check "source state good" grep -q 'state=good' <<< "$out"

step "namespace and table via Polaris API"
check "ns-create $NS" "$S/polaris-api.sh" ns-create "$NS"
check "table-create $NS.polaris_t" "$S/polaris-api.sh" table-create "$NS" polaris_t
# New namespaces reach Dremio's catalog tree with the next names refresh (namesRefreshMs=60s here).
check "Dremio lists $SRC.$NS" "$S/source.sh" wait "$SRC" "$SRC.$NS" 150
check "Dremio lists $SRC.$NS.polaris_t" "$S/source.sh" wait "$SRC/$NS" "$SRC.$NS.polaris_t" 150

step "SQL"
check "SELECT Polaris-created table" "$S/sql.sh" "SELECT count(*) AS c FROM $SRC.$NS.polaris_t"
check "CREATE TABLE" "$S/sql.sh" "CREATE TABLE $SRC.$NS.t1 (id INT, name VARCHAR)"
check "INSERT" "$S/sql.sh" "INSERT INTO $SRC.$NS.t1 VALUES (1, 'a'), (2, 'b')"
out="$("$S/sql.sh" "SELECT id, name FROM $SRC.$NS.t1 ORDER BY id" || true)"
echo "$out"
check "SELECT returns 2 rows" grep -q 'rows=2' <<< "$out"
# shellcheck disable=SC2016  # expanded by the inner bash
check "Polaris lists t1" bash -c '"$1/polaris-api.sh" tables "$2" 2>/dev/null | grep -qx "$2.t1"' _ "$S" "$NS"
check "DROP TABLE t1" "$S/sql.sh" "DROP TABLE $SRC.$NS.t1"
"$S/s3.sh" count

step "secret scan"
check "scan-secrets 0 hits" "$S/scan-secrets.sh"

step "result"
if [ "$failures" -eq 0 ]; then echo "SMOKE PASS (INSTANCE=$INSTANCE)"; else echo "SMOKE FAIL: $failures check(s) (INSTANCE=$INSTANCE)"; fi
[ "$failures" -eq 0 ]
