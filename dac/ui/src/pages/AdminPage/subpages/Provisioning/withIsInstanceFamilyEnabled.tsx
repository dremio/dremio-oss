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

import { useFeatureFlag } from "#oss/exports/providers/useFeatureFlag";
import { ENABLE_M6ID_AND_DDV4_ENGINES } from "@inject/featureFlags/flags/ENABLE_M6ID_AND_DDV4_ENGINES";

export const useInstanceFamilyIsEnabled = () => {
  const [enabled, loading] = useFeatureFlag(ENABLE_M6ID_AND_DDV4_ENGINES);
  return !loading && !!enabled;
};

export const withIsInstanceFamilyEnabled =
  <T,>(WrappedComponent: React.ComponentClass) =>
  (props: T) => {
    return (
      <WrappedComponent
        isInstanceFamilyEnabled={useInstanceFamilyIsEnabled()}
        {...props}
      />
    );
  };
