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
  Role as RoleInterface,
  RoleProperties,
} from "../../interfaces/Role.js";

export class Role implements RoleInterface {
  readonly description: RoleInterface["description"];
  readonly id: RoleInterface["id"];
  readonly memberCount: RoleInterface["memberCount"];
  readonly name: RoleInterface["name"];
  readonly roles: RoleInterface["roles"];
  readonly type: RoleInterface["type"];

  constructor(properties: RoleProperties) {
    this.description = properties.description;
    this.id = properties.id;
    this.memberCount = properties.memberCount;
    this.name = properties.name;
    this.roles = properties.roles;
    this.type = properties.type;
  }

  static fromResource(properties: any) {
    return new Role({
      ...properties,
      description: properties.description?.length
        ? properties.description
        : null,
    });
  }
}
