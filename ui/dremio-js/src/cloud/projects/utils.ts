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
import type { ProjectProperties } from "../../interfaces/Project.js";

export const projectEntityToProperties = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  properties: any,
): ProjectProperties => {
  return {
    cloudId: properties.cloudId,
    cloudType: properties.cloudType,
    createdAt: new Date(properties.createdAt),
    createdBy: properties.createdBy,
    credentials: properties.credentials,
    id: properties.id,
    lastStateError: properties.lastStateError || null,
    modifiedAt: new Date(properties.modifiedAt),
    modifiedBy: properties.modifiedBy,
    name: properties.name,
    numberOfEngines: properties.numberOfEngines,
    primaryCatalog: properties.primaryCatalogId,
    projectStore: properties.projectStore,
    state: properties.state,
    type: properties.type,
  };
};
