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
# Call the instance's Polaris REST APIs with a freshly issued OAuth2 token.
#
# Usage: INSTANCE=0 polaris-api.sh [--as <principal>] <command> [args]
#   raw METHOD PATH [JSON]     PATH is relative to /api/, e.g. catalog/v1/e2ecat/namespaces
#                              or management/v1/catalogs
#   ns-list [PARENT]           list namespaces (dotted names)
#   ns-create NS               create namespace NS (dotted for nested, e.g. a.b)
#   ns-drop NS                 drop namespace NS
#   tables NS                  list tables in NS
#   table-create NS NAME       create table NS.NAME (id long, data string)
#   table-drop NS NAME [purge] drop table (purgeRequested=true when 'purge' is given)
#   table-get NS NAME          print the table metadata-location
#   views NS                   list views in NS
#   token                      only refresh the token header file
# --as <principal> uses $WORK/principal-<principal>.cred (polaris-principal.sh).
# Output: body on stdout, "HTTP <code>" on stderr. Exit 0 on 2xx, 22 otherwise.
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd curl jq

if [ "${1:-}" = "--as" ]; then
  who="${2:?--as needs a principal name}"
  shift 2
  f="$WORK/principal-$who.cred"
  [ -s "$f" ] || die "missing $f"
  E2E_PRINCIPAL_SECRET="$(cut -d: -f2- "$f")"
  export E2E_PRINCIPAL_SECRET
  HDR="$(polaris_token "$(cut -d: -f1 "$f")" E2E_PRINCIPAL_SECRET)"
else
  HDR="$(polaris_token)"
fi

OUT="$WORK/.papi.$$.json"
trap 'rm -f "$OUT"' EXIT
CAT_API="$POLARIS_URL/api/catalog/v1/$POLARIS_CATALOG"

ns_path() { jq -rn --arg ns "$1" '$ns | split(".") | join("\u001f") | @uri'; }

finish() { # finish CODE [jq filter]
  local code="$1" filter="${2:-.}"
  if [ -s "$OUT" ]; then { jq -r "$filter" "$OUT" 2>/dev/null || cat "$OUT"; } | redact; fi
  echo "HTTP $code" >&2
  case "$code" in 2??) exit 0 ;; *) exit 22 ;; esac
}

CMD="${1:?usage: polaris-api.sh [--as p] raw|ns-list|ns-create|ns-drop|tables|table-create|table-drop|table-get|views|token}"
shift
case "$CMD" in
  token)
    echo "HTTP 200" >&2
    ;;
  raw)
    m="${1:?METHOD}" p="${2:?PATH}"
    if [ "$#" -ge 3 ]; then
      code="$(printf '%s' "$3" | http "$m" "$POLARIS_URL/api/$p" "$OUT" "$HDR" 1)"
    else
      code="$(http "$m" "$POLARIS_URL/api/$p" "$OUT" "$HDR")"
    fi
    finish "$code"
    ;;
  ns-list)
    q=""
    [ -n "${1:-}" ] && q="?parent=$(ns_path "$1")"
    code="$(http GET "$CAT_API/namespaces$q" "$OUT" "$HDR")"
    finish "$code" 'if .namespaces then .namespaces[] | join(".") else . end'
    ;;
  ns-create)
    code="$(jq -n --arg ns "${1:?NS}" '{namespace: ($ns | split(".")), properties: {}}' |
      http POST "$CAT_API/namespaces" "$OUT" "$HDR" 1)"
    finish "$code" 'if .namespace then "created " + (.namespace | join(".")) else . end'
    ;;
  ns-drop)
    code="$(http DELETE "$CAT_API/namespaces/$(ns_path "${1:?NS}")" "$OUT" "$HDR")"
    finish "$code"
    ;;
  tables | views)
    code="$(http GET "$CAT_API/namespaces/$(ns_path "${1:?NS}")/$CMD" "$OUT" "$HDR")"
    finish "$code" 'if .identifiers then .identifiers[] | ((.namespace + [.name]) | join(".")) else . end'
    ;;
  table-create)
    ns="${1:?NS}" name="${2:?NAME}"
    code="$(jq -n --arg n "$name" '{name: $n, schema: {type: "struct", "schema-id": 0, fields: [
        {id: 1, name: "id", required: false, type: "long"},
        {id: 2, name: "data", required: false, type: "string"}]}}' |
      http POST "$CAT_API/namespaces/$(ns_path "$ns")/tables" "$OUT" "$HDR" 1)"
    finish "$code" 'if ."metadata-location" then "created \(."metadata-location")" else . end'
    ;;
  table-drop)
    ns="${1:?NS}" name="${2:?NAME}" purge=false
    [ "${3:-}" = purge ] && purge=true
    code="$(http DELETE "$CAT_API/namespaces/$(ns_path "$ns")/tables/$name?purgeRequested=$purge" "$OUT" "$HDR")"
    finish "$code"
    ;;
  table-get)
    ns="${1:?NS}" name="${2:?NAME}"
    code="$(http GET "$CAT_API/namespaces/$(ns_path "$ns")/tables/$name" "$OUT" "$HDR")"
    finish "$code" 'if ."metadata-location" then ."metadata-location" else . end'
    ;;
  *)
    die "unknown command: $CMD"
    ;;
esac
