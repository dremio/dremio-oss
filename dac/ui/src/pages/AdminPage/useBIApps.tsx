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
import {
  CLIENT_TOOLS_POWERBI,
  CLIENT_TOOLS_TABLEAU,
} from "#oss/exports/endpoints/SupportFlags/supportFlagConstants";

const usePowerBI = async () => {
  return await getSupportFlag<boolean>(CLIENT_TOOLS_POWERBI);
};

const useTableau = async () => {
  return await getSupportFlag<boolean>(CLIENT_TOOLS_TABLEAU);
};

export const withBIApps =
  <T,>(WrappedComponent: React.ComponentClass) =>
  (props: T) => {
    return (
      <WrappedComponent
        powerBI={usePowerBI()}
        tableau={useTableau()}
        {...props}
      />
    );
  };
