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

import { Err, Ok, type Result } from "ts-results-es";
import type { SonarV3Config } from "../../_internal/types/Config.js";
import { Role } from "./Role.js";
import type { SignalParam } from "../../_internal/types/Params.js";

export const RolesResource = (config: SonarV3Config) => {
  return {
    retrieve: (
      id: string,
      { signal }: SignalParam = {},
    ): Promise<Result<Role, unknown>> =>
      config
        .sonarV3Request(`role/${id}`, { signal })
        .then((res) => res.json())
        .then((properties) => Ok(Role.fromResource(properties)))
        .catch((e) => Err(e)),
    retrieveByName: (
      name: string,
      { signal }: SignalParam = {},
    ): Promise<Result<Role, unknown>> =>
      config
        .sonarV3Request(`role/by-name/${name}`, { signal })
        .then((res) => res.json())
        .then((properties) => Ok(Role.fromResource(properties)))
        .catch((e) => Err(e)),
  };
};
