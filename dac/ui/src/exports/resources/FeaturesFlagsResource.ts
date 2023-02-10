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

import { SmartResource } from "smart-resource";
import { getFeatureFlagEnabled } from "../endpoints/Features/getFeatureFlagEnabled";
import { Flag } from "../flags/Flag.type";

const featureFlags = new Map<string, boolean>();
const pendingRequests = new Set<string>();

/**
 * The "backend" for this resource is internally stitched together by the UI from
 * multiple lazily-loaded backend requests. Flags should be loaded with the
 * `loadFeatureFlags` function, which will then trigger a refresh of the resource
 * every time the `featureFlags` map is updated.
 */
export const FeaturesFlagsResource = new SmartResource(
  () => new Map(featureFlags)
);

/**
 * Loads the requested flag into `FeaturesFlagsResource`. You must subscribe
 * to the resource to receive updates once the flag has been loaded.
 */
export const loadFeatureFlags = (flag: Flag): Promise<void> | undefined => {
  const flags = Array.isArray(flag) ? flag : [flag];

  const unfetchedFlags = flags.filter(
    (flag) => !featureFlags.has(flag) && !pendingRequests.has(flag)
  );

  if (unfetchedFlags.length) {
    return (
      Promise.all(
        unfetchedFlags.map((unfetchedFlag) => {
          return getFeatureFlagEnabled(unfetchedFlag)
            .then((enabled) => featureFlags.set(unfetchedFlag, enabled))
            .catch(() => featureFlags.set(unfetchedFlag, false))
            .finally(() => {
              pendingRequests.delete(unfetchedFlag);
            });
        })
      )
        // Trigger a refresh on the resource now that we've updated the map
        .then(() => FeaturesFlagsResource.fetch())
        .catch(() => FeaturesFlagsResource.fetch())
    );
  }
};
