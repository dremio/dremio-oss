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

import { useEffect } from "react";
import { useResourceSnapshot } from "smart-resource/react";
import {
  FeaturesFlagsResource,
  loadFeatureFlags,
} from "../resources/FeaturesFlagsResource";
import { type Flag } from "../flags/Flag.type";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

export const useFeatureFlag = (
  flag: Flag
): [result: boolean | null, loading: boolean] => {
  const [features] = useResourceSnapshot(FeaturesFlagsResource);
  const isOSS = !getSonarContext()?.getSelectedProjectId;

  useEffect(() => {
    loadFeatureFlags(flag);
  }, [flag]);

  if (isOSS) return [false, false];

  const flags = Array.isArray(flag) ? flag : [flag];

  const loading =
    features === null || flags.some((flag) => !features.has(flag));
  const result = !loading && flags.every((flag) => features.get(flag));

  return [result, loading];
};
