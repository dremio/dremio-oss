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
import { defineRoute } from "define-route";

type BillingAccountIdParam = { billingAccountId: string };
type UserIdParam = { userId: string };
type RoleIdParam = { roleId: string };
type InviteParam = { inviteId: string };

export const organization = defineRoute(() => "/organization");

/** Org Settings */
export const setting = organization.extend(() => "settings");
export const general = setting.extend(() => "general");
export const projects = setting.extend(() => "projects");
export const clouds = setting.extend(() => "clouds");
export const billing = setting.extend(() => "billing");
export const billingAccountId = billing.extend(
  (params: BillingAccountIdParam) => `${params.billingAccountId}`
);
export const usage = setting.extend(() => "usage");
export const authentication = setting.extend(() => "authentication");
export const biApplications = setting.extend(() => "bi-applications");
export const externalTokens = setting.extend(() => "external-tokens");
export const oauthApps = setting.extend(() => "oauth-applications");
export const users = setting.extend(() => "users");
export const userId = users.extend((params: UserIdParam) => `${params.userId}`);
export const roles = setting.extend(() => "roles");
export const roleId = roles.extend((params: RoleIdParam) => `${params.roleId}`);
export const errorHandling = setting.extend(() => "*");

export const invite = defineRoute(
  (params: InviteParam) => `/invite/${params.inviteId}`
);
export const returningUser = defineRoute(() => "/returning-user");
