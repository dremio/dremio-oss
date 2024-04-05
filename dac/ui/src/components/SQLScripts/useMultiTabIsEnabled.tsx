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
import { useSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { SQLRUNNER_TABS_UI } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { forwardRef } from "react";

export const useMultiTabIsEnabled = () => {
  const [enabled, loading] = useSupportFlag(SQLRUNNER_TABS_UI);
  return !loading && !!enabled;
};

export const MultiTabIsEnabledProvider = (props: any) => {
  const enabled = useMultiTabIsEnabled();
  if (!enabled) return null;
  return props.children;
};

export const withIsMultiTabEnabled = <T,>(
  WrappedComponent: React.ComponentClass,
) =>
  forwardRef((props: T, ref: any) => {
    return (
      <WrappedComponent
        isMultiTabEnabled={useMultiTabIsEnabled()}
        {...props}
        ref={ref}
      />
    );
  });
