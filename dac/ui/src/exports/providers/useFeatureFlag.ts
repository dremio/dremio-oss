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
import { FeaturesFlagsResource } from "../resources/FeaturesFlagsResource";
import { type Flag } from "../flags/Flag.type";

export const useFeatureFlag = (
  flag: Flag
): [result: boolean | null, loading: boolean] => {
  const [features] = useResourceSnapshot(FeaturesFlagsResource);

  const isArray = Array.isArray(flag);

  useEffect(() => {
    if (isArray) {
      flag.forEach((feature) => {
        FeaturesFlagsResource.fetch(feature);
      });
    } else {
      FeaturesFlagsResource.fetch(flag);
    }
  }, [flag, isArray]);

  if (isArray) {
    const loading =
      features === null || !flag.some((feature) => features.has(feature));

    const result = !loading && flag.every((feature) => features.get(feature));

    return [result, loading];
  }

  const loading = features === null || !features.has(flag);
  const result = !loading && !!features.get(flag);

  return [result, loading];
};
