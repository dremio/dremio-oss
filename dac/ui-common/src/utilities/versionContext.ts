/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { VersionContext } from "../types/VersionContext.types";

export function getShortHash(hash?: string) {
  return hash && hash.length > 6 ? hash.substring(0, 8) : hash;
}

export function isDefaultBranch(versionContext?: VersionContext) {
  return versionContext?.type === "BRANCH" && versionContext.value === "main";
}

export function hideForNonDefaultBranch(versionContext?: VersionContext) {
  return !versionContext || isDefaultBranch(versionContext);
}
