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

import { CATALOG_ARS_ENABLED } from "@app/exports/flags/CATALOG_ARS_ENABLED";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { isDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";

export function useIsBIToolsEnabled(versionContext: any) {
  const [arsEnabled, loading] = useFeatureFlag(CATALOG_ARS_ENABLED);
  // Disable Analyze With when catalog_ars_enabled is on and user is not on default branch
  return !loading && (!arsEnabled || isDefaultBranch(versionContext));
}
