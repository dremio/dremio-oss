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
# Remove the Polaris container of INSTANCE. Polaris uses in-memory persistence, so all catalog
# state is gone afterwards. Objects in S3 are not touched (see s3.sh clean).
#
# Usage: INSTANCE=0 polaris-down.sh
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_cmd docker
if docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1; then
  log "removed $POLARIS_CONTAINER"
else
  log "$POLARIS_CONTAINER not present"
fi
rm -f "$WORK"/polaris-*.hdr "$WORK"/principal-*.cred
