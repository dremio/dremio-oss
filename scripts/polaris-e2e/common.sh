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
# Shared settings and helpers for the Polaris / Dremio E2E harness. Sourced by the other scripts.
# Every value is derived from INSTANCE (0..9) so several isolated stacks can run side by side.
# Secrets are read only from the environment, are never echoed, and are never placed on a
# command line (they reach curl/docker/mc through stdin or environment variables).

# shellcheck disable=SC2034  # variables are used by the scripts that source this file

INSTANCE="${INSTANCE:-0}"
case "$INSTANCE" in
  [0-9]) ;;
  *) echo "[e2e] INSTANCE must be a single digit 0..9 (got '$INSTANCE')" >&2; exit 2 ;;
esac

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$E2E_DIR/../.." && pwd)"

# ---- ports (offset = INSTANCE * 10; Dremio ports avoid the product defaults) ----
PORT_OFFSET=$((INSTANCE * 10))
POLARIS_HTTP_PORT=$((18181 + PORT_OFFSET))
POLARIS_MGMT_PORT=$((18182 + PORT_OFFSET))
DREMIO_WEB_PORT=$((19047 + PORT_OFFSET))
DREMIO_CLIENT_PORT=$((31110 + PORT_OFFSET))
DREMIO_FLIGHT_PORT=$((32110 + PORT_OFFSET))
# Below the Linux ephemeral port range (32768-60999), so that outgoing connections cannot hold it.
DREMIO_FABRIC_PORT=$((27710 + PORT_OFFSET))
DREMIO_ZK_PORT=$((12181 + PORT_OFFSET))

# ---- local state ----
WORK="${E2E_WORK:-/tmp/polaris-e2e}/$INSTANCE"
DREMIO_HOME_DIR="$WORK/dremio"
NAME_PREFIX="${E2E_NAME_PREFIX:-p3-}"
POLARIS_CONTAINER="${NAME_PREFIX}polaris-e2e-$INSTANCE"
POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris:1.1.0-incubating}"

# ---- Polaris ----
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_ROOT_ID="${POLARIS_ROOT_ID:-root}"
# Local-test-only bootstrap secret of the throwaway Polaris container (in-memory persistence).
POLARIS_ROOT_SECRET="${POLARIS_ROOT_SECRET:-s3cr3t}"
export POLARIS_ROOT_SECRET
POLARIS_CATALOG="${POLARIS_CATALOG:-e2ecat}"
POLARIS_CATALOG_ROLE="${POLARIS_CATALOG_ROLE:-e2e_admin}"
POLARIS_URL="http://127.0.0.1:$POLARIS_HTTP_PORT"
POLARIS_REST_URI="$POLARIS_URL/api/catalog"
POLARIS_TOKEN_URI="$POLARIS_URL/api/catalog/v1/oauth/tokens"
POLARIS_MGMT_API="$POLARIS_URL/api/management/v1"

# ---- S3 / MinIO ----
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_BUCKET="${S3_BUCKET:-dremiodev}"
S3_PREFIX="${S3_PREFIX:-polaris-e2e-$INSTANCE}"
S3_PREFIX="${S3_PREFIX#/}"
S3_PREFIX="${S3_PREFIX%/}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_BASE_LOCATION="s3://$S3_BUCKET/$S3_PREFIX"
# Dremio's S3A endpoint takes host:port without a scheme; Polaris needs the full URL.
S3_HOSTPORT="${S3_ENDPOINT#*://}"
S3_HOSTPORT="${S3_HOSTPORT%/}"
case "$S3_ENDPOINT" in
  https://*) S3_SSL=true ;;
  *) S3_SSL=false ;;
esac

# ---- Dremio ----
DREMIO_USER="${DREMIO_USER:-dremio}"
DREMIO_PASSWORD="${DREMIO_PASSWORD:-dremio123}"   # local-test-only first user
DREMIO_URL="http://127.0.0.1:$DREMIO_WEB_PORT"
DREMIO_HEAP_MB="${DREMIO_HEAP_MB:-2048}"
DREMIO_DIRECT_MB="${DREMIO_DIRECT_MB:-2048}"
DREMIO_JAVA_HOME="${DREMIO_JAVA_HOME:-$HOME/jvm/jdk-17.0.9}"

umask 077

log() { printf '[e2e:%s] %s\n' "$INSTANCE" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

require_cmd() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "missing command: $c"
  done
}

# S3 credentials come only from the environment (S3_* or AWS_*). No defaults.
require_s3_creds() {
  S3_ACCESS_KEY="${S3_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-}}"
  S3_SECRET_KEY="${S3_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-${MINIO_SECRET_KEY:-}}}"
  [ -n "$S3_ACCESS_KEY" ] || die "set S3_ACCESS_KEY (or AWS_ACCESS_KEY_ID)"
  [ -n "$S3_SECRET_KEY" ] || die "set S3_SECRET_KEY (or AWS_SECRET_ACCESS_KEY / MINIO_SECRET_KEY)"
  export S3_ACCESS_KEY S3_SECRET_KEY
}

# The user's ~/.docker/config.json may be unreadable; drop only that warning.
docker() { command docker "$@" 2> >(grep -v '^WARNING: Error loading config file' >&2); }

ensure_work() { mkdir -p "$WORK"; }

port_in_use() { ss -Hltn "sport = :$1" 2>/dev/null | grep -q .; }

require_free_ports() {
  local p
  for p in "$@"; do
    if port_in_use "$p"; then die "port $p is already in use (INSTANCE=$INSTANCE)"; fi
  done
}

# Replace known secret values with <redacted> (literal match, values read from the environment and
# from the principal credential files of this instance). For an "id:secret" value the bare secret
# is masked as well.
redact() {
  local f files=""
  for f in "$WORK"/principal-*.cred; do
    [ -s "$f" ] && files="$files$(cut -d: -f2- "$f")"$'\n'
  done
  E2E_REDACT_FILE_SECRETS="$files" awk '
       function add(v,   k) {
         if (length(v) >= 4 && !(v in seen)) { seen[v] = 1; s[++m] = v }
         k = index(v, ":")
         if (k > 0) { v = substr(v, k + 1); if (length(v) >= 4 && !(v in seen)) { seen[v] = 1; s[++m] = v } }
       }
       BEGIN {
         n = split("S3_SECRET_KEY AWS_SECRET_ACCESS_KEY MINIO_SECRET_KEY POLARIS_ROOT_SECRET " \
                   "E2E_SRC_CREDENTIAL SOURCE_CREDENTIAL E2E_PRINCIPAL_SECRET E2E_EXTRA_SECRET", names, " ")
         for (i = 1; i <= n; i++) add(ENVIRON[names[i]])
         c = split(ENVIRON["E2E_REDACT_FILE_SECRETS"], fs, "\n")
         for (i = 1; i <= c; i++) add(fs[i])
       }
       {
         line = $0
         for (j = 1; j <= m; j++) {
           out = ""
           while ((k = index(line, s[j])) > 0) {
             out = out substr(line, 1, k - 1) "<redacted>"
             line = substr(line, k + length(s[j]))
           }
           line = out line
         }
         print line
       }'
}

# http METHOD URL OUTFILE [HEADERFILE] [BODY_FROM_STDIN=0|1] -> prints the HTTP status code.
# Request bodies are read from stdin so secrets never appear in argv.
http() {
  local method="$1" url="$2" out="$3" hdr="${4:-}" body="${5:-0}"
  local args=(-sS -X "$method" -o "$out" -w '%{http_code}' --max-time "${HTTP_TIMEOUT:-120}")
  local code
  if [ -n "$hdr" ]; then args+=(-H "@$hdr"); fi
  if [ "$body" = 1 ]; then
    args+=(-H 'Content-Type: application/json' --data-binary @-)
  fi
  code="$(curl "${args[@]}" "$url" 2>/dev/null)" || true
  printf '%s\n' "${code:-000}"
}

wait_for() { # wait_for SECONDS DESCRIPTION CMD...
  local timeout="$1" what="$2" i=0
  shift 2
  until "$@" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge "$timeout" ] && return 1
    sleep 1
  done
  log "$what ready after ${i}s"
}

# ---- Polaris auth ----
# polaris_token [CLIENT_ID [CLIENT_SECRET_ENV_NAME]] -> writes a curl header file, prints its path.
polaris_token() {
  local cid="${1:-$POLARIS_ROOT_ID}" secret_var="${2:-POLARIS_ROOT_SECRET}" scope="${POLARIS_SCOPE:-PRINCIPAL_ROLE:ALL}"
  local hdr="$WORK/polaris-$cid.hdr" out code
  ensure_work
  out="$(mktemp "$WORK/.tok.XXXXXX")"
  code="$(E2E_CID="$cid" E2E_SCOPE="$scope" E2E_SEC="${!secret_var:-}" \
    jq -jn '"grant_type=client_credentials&client_id=\(env.E2E_CID|@uri)&client_secret=\(env.E2E_SEC|@uri)&scope=\(env.E2E_SCOPE|@uri)"' |
    curl -sS -o "$out" -w '%{http_code}' --max-time 30 -X POST \
      -H 'Content-Type: application/x-www-form-urlencoded' --data-binary @- "$POLARIS_TOKEN_URI" 2>/dev/null)" || true
  if [ "$code" != 200 ]; then
    rm -f "$out"
    die "Polaris token request for '$cid' failed: HTTP $code"
  fi
  printf 'Authorization: Bearer %s\n' "$(jq -r .access_token "$out")" > "$hdr"
  rm -f "$out"
  printf '%s\n' "$hdr"
}

# ---- Dremio auth ----
dremio_hdr() {
  [ -s "$WORK/dremio.hdr" ] || die "no Dremio token; run dremio-up.sh first (INSTANCE=$INSTANCE)"
  printf '%s\n' "$WORK/dremio.hdr"
}

dremio_login() {
  local out code
  out="$(mktemp "$WORK/.login.XXXXXX")"
  code="$(E2E_U="$DREMIO_USER" E2E_P="$DREMIO_PASSWORD" jq -n '{userName: env.E2E_U, password: env.E2E_P}' |
    http POST "$DREMIO_URL/apiv2/login" "$out" "" 1)"
  if [ "$code" != 200 ]; then rm -f "$out"; return 1; fi
  jq -r .token "$out" > "$WORK/token"
  printf 'Authorization: _dremio%s\n' "$(cat "$WORK/token")" > "$WORK/dremio.hdr"
  rm -f "$out"
}
