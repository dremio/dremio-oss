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
  CommunityUser,
  CommunityUserProperties,
} from "../../interfaces/User.js";
import type { components } from "../../../apiTypes/v2/endpoints/user.js";
type UserEntity = components["schemas"]["User"];

export class User implements CommunityUser {
  email: CommunityUserProperties["email"];
  readonly id: CommunityUserProperties["id"];
  familyName: CommunityUserProperties["familyName"];
  givenName: CommunityUserProperties["givenName"];
  status: CommunityUserProperties["status"];
  readonly username: CommunityUserProperties["username"];

  constructor(properties: CommunityUserProperties) {
    this.email = properties.email;
    this.id = properties.id;
    this.familyName = properties.familyName;
    this.givenName = properties.givenName;
    this.status = properties.status;
    this.username = properties.username;
  }

  get displayName(): string {
    if (this.givenName?.length || this.familyName?.length) {
      return [this.givenName, this.familyName].filter(Boolean).join(" ");
    }

    if (this.username?.length) {
      return this.username;
    }

    if (this.email?.length) {
      return this.email;
    }

    return this.id;
  }

  get initials(): string {
    const [first, second] = this.displayName.split(" ");

    if (!second) {
      return first!.slice(0, 2);
    }

    return first!.charAt(0) + second.charAt(0);
  }

  static fromResource(userEntity: UserEntity): User {
    return new User({
      email: userEntity.email,
      familyName: userEntity.lastName || null,
      givenName: userEntity.firstName || null,
      id: userEntity.id,
      status: userEntity.active ? "ACTIVE" : "INACTIVE",
      username: userEntity.name,
    });
  }
}
