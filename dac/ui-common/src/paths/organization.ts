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
export const settings = organization.extend(() => "settings");
export const general = settings.extend(() => "general");
export const projects = settings.extend(() => "projects");
export const clouds = settings.extend(() => "clouds");
export const billing = settings.extend(() => "billing");
export const billingAccountId = billing.extend(
  (params: BillingAccountIdParam) => `${params.billingAccountId}`,
);
export const usage = settings.extend(() => "usage");
export const authentication = settings.extend(() => "authentication");
export const biApplications = settings.extend(() => "bi-applications");
export const externalTokens = settings.extend(() => "external-tokens");
export const oauthApps = settings.extend(() => "oauth-applications");
export const auditing = settings.extend(() => "auditing");
export const users = settings.extend(() => "users");
export const userId = users.extend((params: UserIdParam) => `${params.userId}`);
export const roles = settings.extend(() => "roles");
export const privileges = settings.extend(() => "privileges");
export const roleId = roles.extend((params: RoleIdParam) => `${params.roleId}`);
export const errorHandling = settings.extend(() => "*");

export const invite = defineRoute(
  (params: InviteParam) => `/invite/${params.inviteId}`,
);
export const returningUser = defineRoute(() => "/returning-user");
