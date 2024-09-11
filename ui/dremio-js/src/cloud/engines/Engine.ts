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
import type {
  EngineConfiguration,
  Engine as EngineInterface,
  EngineProperties,
} from "../../interfaces/Engine.js";
import type { SonarV3Config } from "../../_internal/types/Config.js";
import { engineEntityToProperties, enginePropertiesToEntity } from "./utils.js";

export class Engine implements EngineInterface {
  readonly activeReplicas: EngineInterface["activeReplicas"];
  readonly additionalEngineStateInfo: EngineInterface["additionalEngineStateInfo"];
  readonly configuration: EngineInterface["configuration"];
  readonly id: EngineInterface["id"];
  readonly instanceFamily: EngineInterface["instanceFamily"];
  readonly name: EngineInterface["name"];
  readonly state: EngineInterface["state"];
  readonly statusChangedAt: EngineInterface["statusChangedAt"];
  readonly queriedAt: EngineInterface["queriedAt"];

  #config: SonarV3Config;

  constructor(properties: EngineProperties, config: SonarV3Config) {
    this.activeReplicas = properties.activeReplicas;
    this.additionalEngineStateInfo = properties.additionalEngineStateInfo;
    this.configuration = properties.configuration;
    this.id = properties.id;
    this.instanceFamily = properties.instanceFamily;
    this.name = properties.name;
    this.state = properties.state;
    this.statusChangedAt = properties.statusChangedAt;
    this.queriedAt = properties.queriedAt;
    this.#config = config;
  }

  update(
    properties: Partial<EngineConfiguration>,
  ): Promise<Result<EngineInterface, unknown>> {
    return this.#config
      .sonarV3Request(`engines/${this.id}`, {
        body: JSON.stringify({
          ...enginePropertiesToEntity(this.configuration),
          ...enginePropertiesToEntity(properties),
        }),
        headers: {
          "Content-Type": "application/json",
        },
        method: "PUT",
      })
      .then((res) => res.json())
      .then((properties) =>
        Ok(new Engine(engineEntityToProperties(properties), this.#config)),
      )
      .catch((e) => Err(e));
  }
}
