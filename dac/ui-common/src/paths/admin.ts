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
type RolesParam = { roleId: string };
type UserIdParam = { userId: string };
type EngineIdParam = { engineId?: string };

import { projectBase } from "./common";
/** Admin Settings */
export const admin = projectBase.extend(() => "settings");
export const info = admin.extend(() => "info");
export const nodeActivity = admin.extend(() => "node-activity");
export const reflections = admin.extend(() => "reflections");
export const users = admin.extend(() => "users");
export const userId = users.extend((params: UserIdParam) => `${params.userId}`);
export const advanced = admin.extend(() => "advanced");
export const engines = admin.extend(
  (params: EngineIdParam) =>
    `engines${
      params?.engineId
        ? "?engineId=" + encodeURIComponent(params.engineId!)
        : ""
    }`
);
export const activation = admin.extend(() => "activation");
export const support = admin.extend(() => "support");
export const queues = admin.extend(() => "queues");
export const engineRouting = admin.extend(() => "engine-routing");
export const roles = admin.extend(() => "roles");
export const roleId = roles.extend((params: RolesParam) => `${params.roleId}`);
export const biApplications = admin.extend(() => "bi-applications");
export const sql = admin.extend(() => "sql");
export const general = admin.extend(() => "general");
export const errorHandling = admin.extend(() => "*");
