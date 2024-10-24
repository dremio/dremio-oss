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
import { Ok } from "ts-results-es";
import type {
  ResourceConfig,
  SonarV3Config,
} from "../../_internal/types/Config.js";
import { Engine } from "./Engine.js";
import { engineEntityToProperties } from "./utils.js";
import type { SignalParam } from "../../_internal/types/Params.js";

export const EnginesResource = (config: ResourceConfig & SonarV3Config) => {
  return {
    /**
     * @hidden
     * @internal
     */
    _createFromEntity: (properties: unknown) =>
      new Engine(engineEntityToProperties(properties), config),
    list: () => {
      return {
        async *data({ signal }: SignalParam = {}) {
          yield* await config
            .sonarV3Request("engines", { signal })
            .then((res) => res.json())
            .then((engines) =>
              engines.map(
                (properties: unknown) =>
                  new Engine(engineEntityToProperties(properties), config),
              ),
            );
        },
      };
    },

    retrieve: (id: string) =>
      config
        .sonarV3Request(`engines/${id}`)
        .then((res) => res.json())
        .then((properties) =>
          Ok(new Engine(engineEntityToProperties(properties), config)),
        ),
  };
};
