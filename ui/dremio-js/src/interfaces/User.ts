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
import type { RoleReference } from "./Role.js";

export type CommunityUserProperties = {
  email: string;
  id: string;
  familyName: string | null;
  givenName: string | null;
  status: "ACTIVE" | "INACTIVE";
  username: string;
};

export type CommunityUserMethods = {
  get displayName(): string;
  get initials(): string;
};

export type EnterpriseUserProperties = CommunityUserProperties & {
  roles: RoleReference[];
};

export type EnterpriseUserMethods = CommunityUserMethods;

export type CloudUserProperties = EnterpriseUserProperties;

export type CloudUserMethods = EnterpriseUserMethods;

export type CommunityUser = CommunityUserProperties & CommunityUserMethods;
export type EnterpriseUser = EnterpriseUserProperties & EnterpriseUserMethods;
export type CloudUser = CloudUserProperties & CloudUserMethods;
