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
import { User as CommunityUser } from "../../community/users/User.js";
import type {
  EnterpriseUser,
  EnterpriseUserProperties,
} from "../../interfaces/User.js";
import type { components } from "../../../apiTypes/v2/endpoints/user.js";
import type { RoleReference } from "../../interfaces/Role.js";
import { userEntityToProperties } from "./utils.js";
type UserEntity = components["schemas"]["User"];

export class User extends CommunityUser implements EnterpriseUser {
  roles: EnterpriseUser["roles"];

  constructor(properties: EnterpriseUserProperties) {
    super(properties);
    this.roles = properties.roles;
  }

  static fromResource(
    userEntity: UserEntity & { roles: RoleReference[] },
  ): User {
    return new User(userEntityToProperties(userEntity));
  }
}
