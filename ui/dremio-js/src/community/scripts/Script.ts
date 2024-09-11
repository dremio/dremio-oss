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
  CommunityScript,
  CommunityScriptProperties,
} from "../../interfaces/Script.js";
import { Query } from "../../common/Query.js";
import type { SonarV3Config } from "../../_internal/types/Config.js";

export class Script implements CommunityScript {
  readonly createdAt: Date;
  readonly createdBy: string;
  readonly id: string;
  readonly modifiedAt: Date;
  readonly modifiedBy: Date;
  readonly name: string;
  readonly query: Query;
  #config: SonarV3Config;

  constructor(properties: CommunityScriptProperties, config: SonarV3Config) {
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
    name?: CommunityScriptProperties["name"];
    query?: CommunityScriptProperties["query"];
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
      .catch((e) => Err(e));
  }

  static fromResource(properties: any, config: SonarV3Config) {
    return new Script(
      {
        createdAt: new Date(properties.createdAt),
        createdBy: properties.createdBy,
        id: properties.id,
        modifiedAt: new Date(properties.modifiedAt),
        modifiedBy: properties.modifiedBy,
        name: properties.name,
        query: new Query(properties.content, properties.context),
      },
      config,
    );
  }
}
