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
# Start an isolated Polaris (in-memory persistence, S3/MinIO storage) for INSTANCE and create the
# E2E catalog, a catalog role with CATALOG_MANAGE_CONTENT, and bind it to service_admin so that
# root with scope PRINCIPAL_ROLE:ALL can manage content.
#
# Usage: INSTANCE=0 S3_ACCESS_KEY=... S3_SECRET_KEY=... polaris-up.sh
# Extra Polaris/Quarkus settings: POLARIS_EXTRA_ENV='key=value;key2=value2'
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd docker curl jq ss
require_s3_creds
ensure_work

docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
require_free_ports "$POLARIS_HTTP_PORT" "$POLARIS_MGMT_PORT"

# Secrets reach the container as inherited environment variables (name-only -e flags).
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export POLARIS_BOOTSTRAP_CREDENTIALS="$POLARIS_REALM,$POLARIS_ROOT_ID,$POLARIS_ROOT_SECRET"

run_args=(
  -d --name "$POLARIS_CONTAINER" --network host
  -e "QUARKUS_HTTP_PORT=$POLARIS_HTTP_PORT"
  -e "QUARKUS_MANAGEMENT_PORT=$POLARIS_MGMT_PORT"
  -e POLARIS_BOOTSTRAP_CREDENTIALS
  -e "polaris.realm-context.realms=$POLARIS_REALM"
  -e quarkus.otel.sdk.disabled=true
  -e polaris.readiness.ignore-severe-issues=true
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY
  -e "AWS_REGION=$S3_REGION"
)
if [ -n "${POLARIS_EXTRA_ENV:-}" ]; then
  IFS=';' read -r -a extra <<< "$POLARIS_EXTRA_ENV"
  for kv in "${extra[@]}"; do
    [ -n "$kv" ] && run_args+=(-e "$kv")
  done
fi

log "starting $POLARIS_CONTAINER ($POLARIS_IMAGE) http=$POLARIS_HTTP_PORT mgmt=$POLARIS_MGMT_PORT"
docker run "${run_args[@]}" "$POLARIS_IMAGE" >/dev/null

health_up() { curl -sf --max-time 2 "http://127.0.0.1:$POLARIS_MGMT_PORT/q/health" | grep -q '"UP"'; }
if ! wait_for "${POLARIS_START_TIMEOUT:-120}" "Polaris health" health_up; then
  docker logs --tail 30 "$POLARIS_CONTAINER" 2>&1 | redact >&2
  die "Polaris did not become healthy"
fi

HDR="$(polaris_token)"
OUT="$WORK/.polaris-up.json"

expect() { # expect CODE ALLOWED... (ALLOWED is a space separated list)
  local code="$1" what="$2"
  shift 2
  local ok
  for ok in "$@"; do [ "$code" = "$ok" ] && { log "$what: HTTP $code"; return 0; }; done
  log "$what: HTTP $code"
  redact < "$OUT" >&2 || true
  die "$what failed"
}

code="$(jq -n --arg name "$POLARIS_CATALOG" --arg base "$S3_BASE_LOCATION" --arg ep "$S3_ENDPOINT" --arg region "$S3_REGION" '
  {catalog: {name: $name, type: "INTERNAL",
    properties: {"default-base-location": $base, "polaris.config.drop-with-purge.enabled": "true"},
    storageConfigInfo: {storageType: "S3", allowedLocations: [($base + "/")],
                        endpoint: $ep, pathStyleAccess: true, region: $region}}}' |
  http POST "$POLARIS_MGMT_API/catalogs" "$OUT" "$HDR" 1)"
expect "$code" "create catalog $POLARIS_CATALOG ($S3_BASE_LOCATION/)" 201 409

code="$(jq -n --arg r "$POLARIS_CATALOG_ROLE" '{catalogRole: {name: $r}}' |
  http POST "$POLARIS_MGMT_API/catalogs/$POLARIS_CATALOG/catalog-roles" "$OUT" "$HDR" 1)"
expect "$code" "create catalog role $POLARIS_CATALOG_ROLE" 201 409

code="$(jq -n '{grant: {type: "catalog", privilege: "CATALOG_MANAGE_CONTENT"}}' |
  http PUT "$POLARIS_MGMT_API/catalogs/$POLARIS_CATALOG/catalog-roles/$POLARIS_CATALOG_ROLE/grants" "$OUT" "$HDR" 1)"
expect "$code" "grant CATALOG_MANAGE_CONTENT" 200 201

code="$(jq -n --arg r "$POLARIS_CATALOG_ROLE" '{catalogRole: {name: $r}}' |
  http PUT "$POLARIS_MGMT_API/principal-roles/service_admin/catalog-roles/$POLARIS_CATALOG" "$OUT" "$HDR" 1)"
expect "$code" "bind $POLARIS_CATALOG_ROLE to principal role service_admin" 200 201
rm -f "$OUT"

log "Polaris ready: rest=$POLARIS_REST_URI warehouse=$POLARIS_CATALOG base=$S3_BASE_LOCATION/"
