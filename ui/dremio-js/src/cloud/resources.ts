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
  ResourceConfig,
  SonarV3Config,
  V3Config,
} from "../_internal/types/Config.js";
import { CatalogResource } from "../community/catalog/CatalogResource.js";
import { RolesResource } from "../community/users/RolesResource.js";
import { ScriptsResource } from "../community/scripts/ScriptsResource.js";
import { EnginesResource } from "./engines/EnginesResource.js";
import { ProjectsResource } from "./projects/ProjectsResource.js";
import { UsersResource } from "./users/UsersResource.js";
import { JobsResource } from "../community/jobs/JobsResource.js";
import moize from "moize";
import { ReflectionsResource } from "../community/reflections/ReflectionsResource.js";

/**
 * moize is required on these resources because they're recreated every time they're called
 * with a different or the same projectId, and this breaks caching across `batch` collectors
 */

/**
 * @internal
 * @hidden
 */
export const Resources = (
  config: (projectId: string) => ResourceConfig & SonarV3Config & V3Config,
) => ({
  //@ts-ignore
  catalog: moize((projectId: string) => CatalogResource(config(projectId))) as (
    projectId: string,
  ) => ReturnType<typeof CatalogResource>,

  //@ts-ignore
  engines: moize((projectId: string) => EnginesResource(config(projectId))) as (
    projectId: string,
  ) => ReturnType<typeof EnginesResource>,

  //@ts-ignore
  jobs: moize((projectId: string) =>
    JobsResource(config(projectId) as any),
  ) as (projectId: string) => ReturnType<typeof JobsResource>,

  projects: ProjectsResource(config(null as any)),

  //@ts-ignore
  reflections: moize((projectId: string) =>
    ReflectionsResource(config(projectId)),
  ) as (projectId: string) => ReturnType<typeof ReflectionsResource>,

  //@ts-ignore
  roles: moize((projectId: string) => RolesResource(config(projectId))) as (
    projectId: string,
  ) => ReturnType<typeof RolesResource>,

  //@ts-ignore
  scripts: moize((projectId: string) => ScriptsResource(config(projectId))) as (
    projectId: string,
  ) => ReturnType<typeof ScriptsResource>,

  //@ts-ignore
  users: moize((projectId: string) => UsersResource(config(projectId))) as (
    projectId: string,
  ) => ReturnType<typeof UsersResource>,
});
