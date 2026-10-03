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
# List or delete the instance's objects under s3://$S3_BUCKET/$S3_PREFIX/ with mc.
# Uses a throwaway MC_CONFIG_DIR and passes credentials through MC_HOST_<alias> (never ~/.mc,
# never on the command line).
#
# Usage: INSTANCE=0 S3_ACCESS_KEY=... S3_SECRET_KEY=... s3.sh ls|count|clean
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd mc
require_s3_creds
case "$S3_PREFIX" in
  polaris-*) ;;
  *) die "refusing to touch S3_PREFIX='$S3_PREFIX' (must start with 'polaris-')" ;;
esac

MC_CONFIG_DIR="$(mktemp -d)"
export MC_CONFIG_DIR
trap 'rm -rf "$MC_CONFIG_DIR"' EXIT
# mc does not percent-decode MC_HOST credentials, so they are inserted verbatim.
case "$S3_ACCESS_KEY$S3_SECRET_KEY" in
  *@* | */*) die "S3 credentials containing '@' or '/' are not supported by s3.sh" ;;
esac
MC_HOST_e2e="${S3_ENDPOINT%%://*}://$S3_ACCESS_KEY:$S3_SECRET_KEY@$S3_HOSTPORT"
export MC_HOST_e2e
TARGET="e2e/$S3_BUCKET/$S3_PREFIX/"
LIST="$MC_CONFIG_DIR/list"

list() { # writes the listing to $LIST, prints the object count; fails on mc errors
  if ! mc ls --recursive "$TARGET" > "$LIST" 2> "$MC_CONFIG_DIR/err"; then
    redact < "$MC_CONFIG_DIR/err" >&2
    die "mc ls failed for s3://$S3_BUCKET/$S3_PREFIX/"
  fi
  wc -l < "$LIST" | tr -d ' '
}

case "${1:-ls}" in
  ls)
    list >/dev/null
    redact < "$LIST"
    ;;
  count)
    echo "objects under s3://$S3_BUCKET/$S3_PREFIX/: $(list)"
    ;;
  clean)
    n="$(list)"
    if [ "$n" -gt 0 ]; then
      mc rm --recursive --force "$TARGET" >/dev/null 2>&1 || true
    fi
    left="$(list)"
    log "deleted $n object(s) under s3://$S3_BUCKET/$S3_PREFIX/, remaining $left"
    [ "$left" -eq 0 ]
    ;;
  *) die "usage: s3.sh ls|count|clean" ;;
esac
