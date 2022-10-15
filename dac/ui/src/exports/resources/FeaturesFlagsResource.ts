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

const createFeatureFetcher = () => {
  const featureFlags = new Map<string, boolean>();
  return async (featureId: string) => {
    try {
      featureFlags.set(featureId, await getFeatureFlagEnabled(featureId));
    } catch (e) {
      featureFlags.set(featureId, false);
    }

    return featureFlags;
  };
};

export const FeaturesFlagsResource = new SmartResource(createFeatureFetcher());
