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
# Run one SQL statement through /api/v3/sql, poll /api/v3/job/{id} until it finishes, then print
# the final state and up to SQL_LIMIT result rows (one JSON object per line).
#
# Usage: INSTANCE=0 sql.sh "SELECT 1"
#        INSTANCE=0 sql.sh -f query.sql        (or SQL on stdin with "-")
# Env: SQL_LIMIT (rows, default 20, max 500), SQL_TIMEOUT (seconds, default 300)
# Exit: 0 when the job is COMPLETED, 1 otherwise (FAILED/CANCELED/timeout/HTTP error).
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd curl jq

case "${1:-}" in
  "") die 'usage: sql.sh "<sql>" | -f <file> | -' ;;
  -f) SQL="$(cat "${2:?missing file}")" ;;
  -) SQL="$(cat)" ;;
  *) SQL="$1" ;;
esac

HDR="$(dremio_hdr)"
OUT="$WORK/.sql.$$.json"
trap 'rm -f "$OUT"' EXIT

code="$(jq -n --arg sql "$SQL" '{sql: $sql}' | http POST "$DREMIO_URL/api/v3/sql" "$OUT" "$HDR" 1)"
if [ "$code" != 200 ]; then
  echo "submit: HTTP $code"
  { jq -r '.errorMessage // .message // empty' "$OUT" 2>/dev/null || true; } | redact
  exit 1
fi
JOB="$(jq -r .id "$OUT")"

deadline=$((SECONDS + ${SQL_TIMEOUT:-300}))
state=""
while :; do
  code="$(http GET "$DREMIO_URL/api/v3/job/$JOB" "$OUT" "$HDR")"
  [ "$code" = 200 ] || { echo "job $JOB: status HTTP $code"; exit 1; }
  state="$(jq -r .jobState "$OUT")"
  case "$state" in COMPLETED | FAILED | CANCELED) break ;; esac
  if [ "$SECONDS" -ge "$deadline" ]; then echo "job $JOB: timeout in state $state"; exit 1; fi
  sleep 0.5
done

rows="$(jq -r '.rowCount // 0' "$OUT")"
echo "job $JOB: $state rows=$rows"
if [ "$state" != COMPLETED ]; then
  jq -r '"error: " + (.errorMessage // "-") + (if (.cancellationReason // "") != "" then "\ncancel: " + .cancellationReason else "" end)' "$OUT" | redact
  exit 1
fi
# Always fetch the results: statements answered on the coordinator without executor fragments
# (SHOW SCHEMAS / SHOW TABLES / DESCRIBE ...) report rowCount=0 in the job status even though the
# results endpoint returns their rows.
code="$(http GET "$DREMIO_URL/api/v3/job/$JOB/results?offset=0&limit=${SQL_LIMIT:-20}" "$OUT" "$HDR")"
[ "$code" = 200 ] || { echo "results: HTTP $code"; exit 1; }
fetched="$(jq -r '.rows | length' "$OUT")"
if [ "$fetched" -gt 0 ] && [ "$rows" = 0 ]; then echo "(job rowCount=0; results endpoint returned $fetched rows)"; fi
jq -c '.rows[]' "$OUT" | redact
