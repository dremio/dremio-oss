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
  SonarV2Config,
  SonarV3Config,
} from "../_internal/types/Config.js";
import { CatalogResource } from "./catalog/CatalogResource.js";
import { JobsResource } from "./jobs/JobsResource.js";
import { ReflectionsResource } from "./reflections/ReflectionsResource.js";
import { RolesResource } from "./users/RolesResource.js";
import { ScriptsResource } from "./scripts/ScriptsResource.js";
import { UsersResource } from "./users/UsersResource.js";

/**
 * @internal
 * @hidden
 */
export const Resources = (
  config: ResourceConfig & SonarV2Config & SonarV3Config,
) => ({
  catalog: CatalogResource(config),
  jobs: JobsResource(config),
  reflections: ReflectionsResource(config),
  roles: RolesResource(config),
  scripts: ScriptsResource(config),
  users: UsersResource(config),
});
