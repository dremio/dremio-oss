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
import { Err, Ok } from "ts-results-es";
import type {
  Script as ScriptInterface,
  ScriptProperties,
} from "../../interfaces/Script.js";
import { Query } from "../../common/Query.js";
import type { SonarV3Config } from "../../_internal/types/Config.js";
import { HttpError } from "../../common/HttpError.js";
import { isProblem } from "../../interfaces/Problem.js";
import { duplicateScriptNameError } from "./ScriptErrors.js";

export class Script implements ScriptInterface {
  readonly createdAt: ScriptProperties["createdAt"];
  readonly createdBy: string;
  readonly id: string;
  readonly modifiedAt: ScriptProperties["modifiedAt"];
  readonly modifiedBy: Date;
  readonly name: string;
  readonly query: Query;
  #config: SonarV3Config;

  constructor(properties: ScriptProperties, config: SonarV3Config) {
    this.createdAt = properties.createdAt;
    this.createdBy = properties.createdBy;
    this.id = properties.id;
    this.modifiedAt = properties.modifiedAt;
    this.modifiedBy = properties.modifiedBy;
    this.name = properties.name;
    this.query = properties.query;
    this.#config = config;
  }

  async delete() {
    return this.#config
      .sonarV3Request(`scripts/${this.id}`, {
        keepalive: true,
        method: "DELETE",
      })
      .then(() => Ok(undefined))
      .catch((e) => Err(e));
  }

  async save(properties: {
    name?: ScriptProperties["name"];
    query?: ScriptProperties["query"];
  }) {
    const patchedFields = {} as any;
    if (properties.name) {
      patchedFields.name = properties.name;
    }
    if (properties.query) {
      patchedFields.content = properties.query.sql;
      patchedFields.context = properties.query.context;
    }

    return this.#config
      .sonarV3Request(`scripts/${this.id}`, {
        body: JSON.stringify(patchedFields),
        headers: {
          "Content-Type": "application/json",
        },
        keepalive: true,
        method: "PATCH",
      })
      .then((res) => res.json())
      .then((properties) => Ok(Script.fromResource(properties, this.#config)))
      .catch((e) => {
        if (e instanceof HttpError) {
          if (isProblem(e.body)) {
            if (e.body.detail?.includes("Cannot reuse the same script name")) {
              return Err(duplicateScriptNameError(patchedFields.name));
            }
            return Err(e.body);
          }
        }

        return Err(e);
      });
  }

  static fromResource(properties: any, config: SonarV3Config) {
    return new Script(
      {
        createdAt: Temporal.Instant.from(properties.createdAt),
        createdBy: properties.createdBy,
        id: properties.id,
        modifiedAt: Temporal.Instant.from(properties.modifiedAt),
        modifiedBy: properties.modifiedBy,
        name: properties.name,
        query: new Query(properties.content, properties.context),
      },
      config,
    );
  }
}
