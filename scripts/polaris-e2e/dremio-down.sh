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
# Stop the Dremio of INSTANCE. Files stay in $WORK/dremio (use --purge to delete them).
#
# Usage: INSTANCE=0 dremio-down.sh [--purge]
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

pidf="$DREMIO_HOME_DIR/run/dremio.pid"
if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
  pid="$(cat "$pidf")"
  log "stopping Dremio (pid $pid)"
  DREMIO_STOP_TIMEOUT="${DREMIO_STOP_TIMEOUT:-60}" "$DREMIO_HOME_DIR/bin/dremio" stop >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  if kill -0 "$pid" 2>/dev/null; then
    log "pid $pid still alive, sending SIGKILL"
    kill -9 "$pid" 2>/dev/null || true
  fi
  log "Dremio stopped"
else
  log "Dremio for INSTANCE=$INSTANCE is not running"
fi
rm -f "$WORK/token" "$WORK/dremio.hdr"
if [ "${1:-}" = "--purge" ]; then
  rm -rf "$DREMIO_HOME_DIR"
  log "removed $DREMIO_HOME_DIR"
fi
