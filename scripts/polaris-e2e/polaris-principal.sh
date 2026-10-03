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
# Create (or re-create) a Polaris principal with its own principal role and catalog role that
# holds only the given privileges, for 401/403 tests. The generated client secret is written to
# $WORK/principal-<name>.cred (mode 600, format client_id:client_secret) and never printed.
#
# Usage: INSTANCE=0 polaris-principal.sh <name> [PRIVILEGE...]
#   default privileges (read-only): CATALOG_READ_PROPERTIES NAMESPACE_LIST NAMESPACE_READ_PROPERTIES
#                                   TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA VIEW_LIST
#                                   VIEW_READ_PROPERTIES
#   Grants are catalog-level ({"type":"catalog"}).
#   Delete: INSTANCE=0 polaris-principal.sh --delete <name>
# Use with source.sh: SOURCE_PRINCIPAL=<name> source.sh create <source>
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd curl jq
ensure_work

delete_only=0
if [ "${1:-}" = "--delete" ]; then delete_only=1; shift; fi
NAME="${1:?usage: polaris-principal.sh [--delete] <name> [PRIVILEGE...]}"
shift
case "$NAME" in
  *[!a-zA-Z0-9_-]*) die "principal name must match [a-zA-Z0-9_-]+" ;;
esac
if [ "$#" -gt 0 ]; then
  PRIVS=("$@")
else
  PRIVS=(CATALOG_READ_PROPERTIES NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST
    TABLE_READ_PROPERTIES TABLE_READ_DATA VIEW_LIST VIEW_READ_PROPERTIES)
fi
PR="${NAME}_pr"
CR="${NAME}_cr"
CRED="$WORK/principal-$NAME.cred"
HDR="$(polaris_token)"
OUT="$WORK/.principal.json"
trap 'rm -f "$OUT"' EXIT

call() { # call METHOD PATH [JSON]
  if [ "$#" -ge 3 ]; then
    printf '%s' "$3" | http "$1" "$POLARIS_MGMT_API$2" "$OUT" "$HDR" 1
  else
    http "$1" "$POLARIS_MGMT_API$2" "$OUT" "$HDR"
  fi
}

# Always start from a clean slate so the generated secret matches the .cred file.
log "delete principal $NAME: HTTP $(call DELETE "/principals/$NAME")"
log "delete principal role $PR: HTTP $(call DELETE "/principal-roles/$PR")"
log "delete catalog role $CR: HTTP $(call DELETE "/catalogs/$POLARIS_CATALOG/catalog-roles/$CR")"
rm -f "$CRED"
[ "$delete_only" = 1 ] && exit 0

code="$(call POST /principals "$(jq -nc --arg n "$NAME" '{principal: {name: $n, properties: {}}, credentialRotationRequired: false}')")"
[ "$code" = 201 ] || { redact < "$OUT" >&2; die "create principal $NAME: HTTP $code"; }
jq -r '.credentials | "\(.clientId):\(.clientSecret)"' "$OUT" > "$CRED"
: > "$OUT"
log "create principal $NAME: HTTP $code (credential saved to $CRED)"

code="$(call POST /principal-roles "$(jq -nc --arg n "$PR" '{principalRole: {name: $n}}')")"
log "create principal role $PR: HTTP $code"
code="$(call PUT "/principals/$NAME/principal-roles" "$(jq -nc --arg n "$PR" '{principalRole: {name: $n}}')")"
log "assign $PR to $NAME: HTTP $code"
code="$(call POST "/catalogs/$POLARIS_CATALOG/catalog-roles" "$(jq -nc --arg n "$CR" '{catalogRole: {name: $n}}')")"
log "create catalog role $CR: HTTP $code"
for p in "${PRIVS[@]}"; do
  code="$(call PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/$CR/grants" "$(jq -nc --arg p "$p" '{grant: {type: "catalog", privilege: $p}}')")"
  log "grant $p to $CR: HTTP $code"
  case "$code" in 200 | 201) ;; *) redact < "$OUT" >&2; die "grant $p failed" ;; esac
done
code="$(call PUT "/principal-roles/$PR/catalog-roles/$POLARIS_CATALOG" "$(jq -nc --arg n "$CR" '{catalogRole: {name: $n}}')")"
log "bind $CR to $PR: HTTP $code"
log "principal $NAME ready (client id: $(cut -d: -f1 "$CRED"))"
