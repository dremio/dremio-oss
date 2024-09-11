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
import { Err, Ok } from "ts-results-es";
import type { ResourceConfig, V3Config } from "../../_internal/types/Config.js";
import { Project } from "./Project.js";
import { projectEntityToProperties } from "./utils.js";

export const ProjectsResource = (config: ResourceConfig & V3Config) => {
  const deleteProject = (id: string) =>
    config
      .v3Request(`projects/${id}`, { method: "DELETE" })
      .then(() => Ok(undefined))
      .catch((e) => Err(e));
  const projectMethods = {
    delete: deleteProject,
  };
  return {
    /**
     * @hidden
     * @internal
     */
    _createFromEntity: (properties: unknown) =>
      new Project(projectEntityToProperties(properties), projectMethods),
    delete: deleteProject,
    list: () => {
      return {
        async *data() {
          yield* await config
            .v3Request("projects")
            .then((res) => res.json())
            .then(
              (projects) =>
                projects.map(
                  (properties: unknown) =>
                    new Project(
                      projectEntityToProperties(properties),
                      projectMethods,
                    ),
                ) as Project[],
            );
        },
      };
    },
    retrieve: (id: string) =>
      config
        .v3Request(`projects/${id}`)
        .then((res) => res.json())
        .then((properties) =>
          Ok(
            new Project(projectEntityToProperties(properties), projectMethods),
          ),
        )
        .catch((e) => Err(e)),
  };
};
