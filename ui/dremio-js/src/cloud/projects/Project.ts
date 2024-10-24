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
import type {
  Project as ProjectInterface,
  ProjectProperties,
} from "../../interfaces/Project.js";

export class Project implements ProjectInterface {
  readonly cloudId: ProjectInterface["cloudId"];
  readonly cloudType: ProjectInterface["cloudType"];
  readonly createdAt: ProjectInterface["createdAt"];
  readonly createdBy: ProjectInterface["createdBy"];
  readonly credentials: ProjectInterface["credentials"];
  readonly id: ProjectInterface["id"];
  readonly lastStateError: ProjectInterface["lastStateError"];
  readonly modifiedAt: ProjectInterface["modifiedAt"];
  readonly modifiedBy: ProjectInterface["modifiedBy"];
  readonly name: ProjectInterface["name"];
  readonly numberOfEngines: ProjectInterface["numberOfEngines"];
  readonly primaryCatalog: ProjectInterface["primaryCatalog"];
  readonly projectStore: ProjectInterface["projectStore"];
  readonly state: ProjectInterface["state"];
  readonly type: ProjectInterface["type"];

  #apiMethods: any;

  constructor(properties: ProjectProperties, apiMethods: any) {
    this.cloudId = properties.cloudId;
    this.cloudType = properties.cloudType;
    this.createdAt = properties.createdAt;
    this.createdBy = properties.createdBy;
    this.credentials = properties.credentials;
    this.id = properties.id;
    this.lastStateError = properties.lastStateError;
    this.modifiedAt = properties.modifiedAt;
    this.modifiedBy = properties.modifiedBy;
    this.name = properties.name;
    this.numberOfEngines = properties.numberOfEngines;
    this.primaryCatalog = properties.primaryCatalog;
    this.projectStore = properties.projectStore;
    this.state = properties.state;
    this.type = properties.type;
    this.#apiMethods = apiMethods;
  }

  async delete() {
    return this.#apiMethods.delete(this.id);
  }

  get settled() {
    return (
      this.state === "ACTIVE" ||
      this.state === "ARCHIVED" ||
      this.state === "INACTIVE"
    );
  }
}
