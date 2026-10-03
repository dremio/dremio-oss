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
# Count occurrences of secret values in the instance's Dremio logs and Polaris container log.
# Only counts are printed, never the values or matching lines.
#
# Usage: INSTANCE=0 scan-secrets.sh [VAR_NAME...]
#   Scans for the values of the named environment variables. Default set: S3_SECRET_KEY
#   (or AWS_SECRET_ACCESS_KEY / MINIO_SECRET_KEY), POLARIS_ROOT_SECRET, plus every client secret in
#   $WORK/principal-*.cred. Values shorter than 4 characters are skipped.
#   Also counts the token/credential markers 'Bearer ', 'access_token', 's3.secret-access-key',
#   's3.session-token' and 'client_secret'. Rotated logs (log/archive/*.gz) are decompressed and
#   scanned too.
# Exit: 0 when everything is 0, 1 otherwise.
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

S3_SECRET_KEY="${S3_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-${MINIO_SECRET_KEY:-}}}"
vars=("$@")
[ "${#vars[@]}" -gt 0 ] || vars=(S3_SECRET_KEY POLARIS_ROOT_SECRET)

ensure_work
TMP="$(mktemp -d "$WORK/.scan.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

targets=()
if [ -d "$DREMIO_HOME_DIR/log" ]; then targets+=("$DREMIO_HOME_DIR/log"); fi
if command -v docker >/dev/null 2>&1 && docker inspect "$POLARIS_CONTAINER" >/dev/null 2>&1; then
  docker logs "$POLARIS_CONTAINER" > "$TMP/polaris-container.log" 2>&1 || true
  targets+=("$TMP/polaris-container.log")
fi
[ "${#targets[@]}" -gt 0 ] || die "nothing to scan (no Dremio log dir, no Polaris container)"

count() { # count PATTERN_FILE TARGET -> number of matches (rotated *.gz logs decompressed)
  {
    grep -rhoF --exclude='*.gz' -f "$1" "$2" 2>/dev/null || true
    find "$2" -type f -name '*.gz' -exec zcat -f {} + 2>/dev/null | { grep -oF -f "$1" || true; }
  } | wc -l | tr -d ' '
}

total=0
check() { # check LABEL PATTERN_FILE
  local label="$1" pf="$2" t n
  for t in "${targets[@]}"; do
    n="$(count "$pf" "$t")"
    total=$((total + n))
    printf '%-34s %-40s %s\n' "$label" "$(basename "$t")" "$n"
  done
}

for v in "${vars[@]}"; do
  val="${!v:-}"
  if [ "${#val}" -lt 4 ]; then
    printf '%-34s %s\n' "secret:$v" "skipped (unset or shorter than 4)"
    continue
  fi
  printf '%s\n' "$val" > "$TMP/p"
  check "secret:$v" "$TMP/p"
done
for f in "$WORK"/principal-*.cred; do
  [ -s "$f" ] || continue
  cut -d: -f2- "$f" > "$TMP/p"
  n="$(basename "$f" .cred)"
  check "secret:${n#principal-}" "$TMP/p"
done
for m in 'Bearer ' 'access_token' 's3.secret-access-key' 's3.session-token' 'client_secret'; do
  printf '%s\n' "$m" > "$TMP/p"
  check "marker:$m" "$TMP/p"
done
echo "total hits: $total"
[ "$total" -eq 0 ]
