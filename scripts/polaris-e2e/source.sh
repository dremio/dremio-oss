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
# Manage a RESTCATALOG source that points at the instance's Polaris, via /api/v3/catalog.
#
# Usage: INSTANCE=0 source.sh <command> [source-name]     (default name: polaris)
#   create   POST the source built from the settings below
#   update   PUT the same body onto the existing source (keeps id/tag)
#   get      print id, state and children
#   ls PATH  list children of a catalog path, e.g. ls polaris/ns1 (slash separated)
#   wait PATH CHILD [SECONDS]  poll "ls PATH" until CHILD (dotted full path) is listed (default 120s)
#   delete   DELETE the source
#   json     print the request body with secrets masked (nothing is sent)
#
# Settings (environment):
#   SOURCE_URI          restEndpointUri            (default http://127.0.0.1:<polaris port>/api/catalog)
#   SOURCE_WAREHOUSE    warehouse property          (default $POLARIS_CATALOG)
#   SOURCE_SCOPE        scope property              (default PRINCIPAL_ROLE:ALL)
#   SOURCE_OAUTH2_URI   oauth2-server-uri property  (default <polaris>/api/catalog/v1/oauth/tokens, '-' omits it)
#   SOURCE_PRINCIPAL    use $WORK/principal-<name>.cred (from polaris-principal.sh) as credential
#   SOURCE_CREDENTIAL   explicit credential value (secret; env only). Default root:$POLARIS_ROOT_SECRET
#   SOURCE_ALLOWED_NS   comma separated allowedNamespaces (dotted, e.g. "a,b.c"). Unset = field omitted
#                       (all namespaces); set to empty = [].
#   SOURCE_RECURSIVE    isRecursiveAllowedNamespaces (default true)
#   SOURCE_VENDED       isUsingVendedCredentials     (default false)
#   SOURCE_NO_S3_KEYS=1 do not send fs.s3a.access.key / fs.s3a.secret.key
#   SOURCE_OMIT_PROPS   comma separated property names to drop (e.g. "warehouse,scope")
#   SOURCE_EXTRA_PROPS  comma separated non-secret k=v properties to add or override
#   SOURCE_KEEP_SECRETS=1 (update) send $DREMIO_EXISTING_VALUE$ for every secret
#   SOURCE_NAMES_REFRESH_MS  metadataPolicy.namesRefreshMs (min 60000). Unset = server default (1h).
#                       Namespaces created in Polaris after the source exists only show up in
#                       Dremio's catalog tree after a names refresh (or after a query touches them).
#   SOURCE_VERBOSE=1    print the full (redacted) response
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd curl jq

CMD="${1:?usage: source.sh create|update|get|ls|wait|delete|json [name]}"
shift
if [ "$CMD" = ls ] || [ "$CMD" = wait ]; then
  LS_PATH="${1:?usage: source.sh ls|wait <path> ...}"
  NAME="${LS_PATH%%/*}"
else
  NAME="${1:-${SOURCE_NAME:-polaris}}"
fi
OUT="$WORK/.source.$$.json"
trap 'rm -f "$OUT"' EXIT

credential_env() {
  if [ -n "${SOURCE_CREDENTIAL:-}" ]; then
    E2E_SRC_CREDENTIAL="$SOURCE_CREDENTIAL"
  elif [ -n "${SOURCE_PRINCIPAL:-}" ]; then
    local f="$WORK/principal-$SOURCE_PRINCIPAL.cred"
    [ -s "$f" ] || die "missing $f (run polaris-principal.sh $SOURCE_PRINCIPAL)"
    E2E_SRC_CREDENTIAL="$(cat "$f")"
  else
    E2E_SRC_CREDENTIAL="$POLARIS_ROOT_ID:$POLARIS_ROOT_SECRET"
  fi
  export E2E_SRC_CREDENTIAL
}

build_body() { # build_body MASK(0|1)
  local mask="$1" allowed_set=0
  [ -n "${SOURCE_ALLOWED_NS+x}" ] && allowed_set=1
  if [ "$mask" = 1 ]; then
    export S3_ACCESS_KEY="<s3AccessKey>" S3_SECRET_KEY="<s3SecretKey>" E2E_SRC_CREDENTIAL="<client_id>:<client_secret>"
  fi
  jq -n \
    --arg name "$NAME" \
    --arg uri "${SOURCE_URI:-$POLARIS_REST_URI}" \
    --arg wh "${SOURCE_WAREHOUSE:-$POLARIS_CATALOG}" \
    --arg scope "${SOURCE_SCOPE:-PRINCIPAL_ROLE:ALL}" \
    --arg oauth "${SOURCE_OAUTH2_URI:-$POLARIS_TOKEN_URI}" \
    --arg s3ep "$S3_HOSTPORT" --arg s3ssl "$S3_SSL" --arg region "$S3_REGION" \
    --arg allowedSet "$allowed_set" --arg allowed "${SOURCE_ALLOWED_NS:-}" \
    --arg recursive "${SOURCE_RECURSIVE:-true}" --arg vended "${SOURCE_VENDED:-false}" \
    --arg noS3 "${SOURCE_NO_S3_KEYS:-0}" \
    --arg omit "${SOURCE_OMIT_PROPS:-}" --arg extra "${SOURCE_EXTRA_PROPS:-}" \
    --arg namesMs "${SOURCE_NAMES_REFRESH_MS:-}" '
    def csv($s): $s | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0));
    ([{name: "warehouse", value: $wh}, {name: "scope", value: $scope}]
      + (if $oauth == "-" then [] else [{name: "oauth2-server-uri", value: $oauth}] end)
      + [{name: "fs.s3a.aws.credentials.provider", value: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"},
         {name: "fs.s3a.endpoint", value: $s3ep},
         {name: "fs.s3a.connection.ssl.enabled", value: $s3ssl},
         {name: "fs.s3a.path.style.access", value: "true"},
         {name: "dremio.s3.compat", value: "true"},
         {name: "dremio.bucket.discovery.enabled", value: "false"},
         {name: "dremio.s3.region", value: $region},
         {name: "fs.s3a.endpoint.region", value: $region},
         {name: "fs.s3a.requester.pays.enabled", value: "false"}]) as $base
    | (csv($extra) | map(capture("^(?<name>[^=]+)=(?<value>.*)$"))) as $extraProps
    | ($extraProps | map(.name)) as $extraNames
    | (csv($omit) + $extraNames) as $drop
    | {entityType: "source", type: "RESTCATALOG", name: $name,
       config: ({restEndpointUri: $uri,
                 isUsingVendedCredentials: ($vended == "true"),
                 isRecursiveAllowedNamespaces: ($recursive == "true"),
                 propertyList: (($base | map(select(.name as $n | $drop | index($n) | not))) + $extraProps),
                 secretPropertyList: ([{name: "credential", value: env.E2E_SRC_CREDENTIAL}]
                   + (if $noS3 == "1" then [] else
                       [{name: "fs.s3a.access.key", value: env.S3_ACCESS_KEY},
                        {name: "fs.s3a.secret.key", value: env.S3_SECRET_KEY}] end)),
                 enableAsync: true, isCachingEnabled: true, maxCacheSpacePct: 100}
               + (if $allowedSet == "1" then {allowedNamespaces: csv($allowed)} else {} end))}
    + (if $namesMs == "" then {} else
        {metadataPolicy: {authTTLMs: 86400000, namesRefreshMs: ($namesMs | tonumber),
                          datasetRefreshAfterMs: 3600000, datasetExpireAfterMs: 10800000,
                          datasetUpdateMode: "PREFETCH_QUERIED", deleteUnavailableDatasets: true,
                          autoPromoteDatasets: false}} end)'
}

summarize() { # summarize ACTION CODE
  local action="$1" code="$2"
  if [ "${SOURCE_VERBOSE:-0}" = 1 ]; then jq . "$OUT" 2>/dev/null | redact || redact < "$OUT"; fi
  case "$code" in
    2??)
      jq -r --arg a "$action" --arg c "$code" --arg n "$NAME" \
        '"\($a) \($n): HTTP \($c) id=\(.id // "-") state=\(.state.status // "-")"
         + (if (.state.messages // []) | length > 0 then "\nstate.messages: " + ((.state.messages | map(.message)) | join(" | ")) else "" end)' \
        "$OUT" 2>/dev/null | redact || echo "$action $NAME: HTTP $code"
      ;;
    *)
      echo "$action $NAME: HTTP $code"
      { jq -r '.errorMessage // .message // empty' "$OUT" 2>/dev/null || head -c 2000 "$OUT"; } | redact
      return 1
      ;;
  esac
}

# Resolve secrets in the main shell (not inside a pipeline) so a missing value stops the script
# before anything is sent.
prepare_secrets() {
  if [ "${SOURCE_KEEP_SECRETS:-0}" = 1 ]; then
    # shellcheck disable=SC2016  # literal Dremio placeholder, not an expansion
    export S3_ACCESS_KEY='$DREMIO_EXISTING_VALUE$' S3_SECRET_KEY='$DREMIO_EXISTING_VALUE$' E2E_SRC_CREDENTIAL='$DREMIO_EXISTING_VALUE$'
    return
  fi
  credential_env
  if [ "${SOURCE_NO_S3_KEYS:-0}" != 1 ]; then require_s3_creds; fi
}

source_json() { # fetch current entity into $OUT, return code
  http GET "$DREMIO_URL/api/v3/catalog/by-path/$1" "$OUT" "$(dremio_hdr)"
}

case "$CMD" in
  json)
    build_body 1 | jq .
    ;;
  create)
    prepare_secrets
    code="$(build_body 0 | http POST "$DREMIO_URL/api/v3/catalog" "$OUT" "$(dremio_hdr)" 1)"
    summarize create "$code"
    ;;
  update)
    prepare_secrets
    code="$(source_json "$NAME")"
    [ "$code" = 200 ] || { summarize get "$code"; exit 1; }
    id="$(jq -r .id "$OUT")"
    tag="$(jq -r .tag "$OUT")"
    code="$(build_body 0 | jq --arg id "$id" --arg tag "$tag" '. + {id: $id, tag: $tag}' |
      http PUT "$DREMIO_URL/api/v3/catalog/$id" "$OUT" "$(dremio_hdr)" 1)"
    summarize update "$code"
    ;;
  get)
    code="$(source_json "$NAME")"
    summarize get "$code"
    jq -r '(.children // [])[] | "child: \(.path | join(".")) [\(.type)\(if .containerType then "/" + .containerType else "" end)]"' "$OUT"
    ;;
  ls)
    code="$(source_json "$LS_PATH")"
    echo "ls $LS_PATH: HTTP $code"
    if [ "$code" = 200 ]; then
      jq -r '(.children // [])[] | "\(.path | join(".")) [\(.type)\(if .containerType then "/" + .containerType elif .datasetType then "/" + .datasetType else "" end)]"' "$OUT"
    else
      { jq -r '.errorMessage // empty' "$OUT" 2>/dev/null || true; } | redact
      exit 1
    fi
    ;;
  wait)
    child="${2:?usage: source.sh wait <path> <child> [seconds]}"
    deadline=$((SECONDS + ${3:-120}))
    while :; do
      code="$(source_json "$LS_PATH")"
      if [ "$code" = 200 ] && jq -e --arg c "$child" '(.children // []) | map(.path | join(".")) | index($c)' "$OUT" >/dev/null; then
        echo "wait $LS_PATH: $child listed after $((SECONDS + ${3:-120} - deadline))s"
        exit 0
      fi
      if [ "$SECONDS" -ge "$deadline" ]; then echo "wait $LS_PATH: $child not listed (last HTTP $code)"; exit 1; fi
      sleep 5
    done
    ;;
  delete)
    # Look the id up in the root listing: GET by-path fails (HTTP 400) for a source in bad state.
    code="$(http GET "$DREMIO_URL/api/v3/catalog" "$OUT" "$(dremio_hdr)")"
    [ "$code" = 200 ] || { summarize list "$code"; exit 1; }
    id="$(jq -r --arg n "$NAME" '[.data[] | select(.containerType == "SOURCE" and .path == [$n]) | .id][0] // empty' "$OUT")"
    if [ -z "$id" ]; then echo "delete $NAME: not found"; exit 0; fi
    code="$(http DELETE "$DREMIO_URL/api/v3/catalog/$id" "$OUT" "$(dremio_hdr)")"
    echo "delete $NAME: HTTP $code"
    case "$code" in 2??) ;; *) exit 1 ;; esac
    ;;
  *)
    die "unknown command: $CMD"
    ;;
esac
