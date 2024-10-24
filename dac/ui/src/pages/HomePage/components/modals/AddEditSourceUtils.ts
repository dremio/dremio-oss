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
import { getSupportFlag } from "#oss/exports/endpoints/SupportFlags/getSupportFlag";
import { REFLECTION_ARCTIC_ENABLED } from "#oss/exports/endpoints/SupportFlags/supportFlagConstants";
import sentryUtil from "#oss/utils/sentryUtil";

// Enable reflection related tab to form if it's a versioned source and the support flag is on
export async function isVersionedReflectionsEnabled() {
  try {
    const res = await getSupportFlag<boolean>(REFLECTION_ARCTIC_ENABLED);
    return res.value === true;
  } catch (e) {
    sentryUtil.logException(e);
  }

  return false;
}
