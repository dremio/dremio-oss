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

import { isDcsEdition } from "dyn-load/utils/versionUtils";

type ProjectIdParam = { projectId: string };
type FilterParams = {
  filter?: string;
  order?: "ASCENDING" | "DESCENDING";
  sort?: string;
};

export const projectBase = (() => {
  if (isDcsEdition()) {
    return (params: ProjectIdParam) =>
      new URL(`/sonar/${params.projectId}/`, window.location.origin);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  return (params: ProjectIdParam) => new URL(``, window.location.origin);
})();

/** Datasets */
export const datasets = (params: ProjectIdParam) =>
  new URL(``, projectBase(params));

/** Jobs  */
type JobIdParam = { jobId: string };
export const jobs = (params: ProjectIdParam & FilterParams) =>
  new URL("jobs/", projectBase(params));
export const job = (params: ProjectIdParam & JobIdParam) =>
  new URL(`job/${params.jobId}`, jobs(params));

/** Listing */

export const projectsList = () => new URL("sonar", window.location.origin);

/** SQL */
type SQLContextParam = { projectContext?: string };
export const sqlBase = (params: ProjectIdParam & SQLContextParam) => {
  const url = new URL("new_query", projectBase(params));

  if (params.projectContext) {
    url.searchParams.set("context", params.projectContext);
  }

  return url;
};

/** Project settings */
export const projectSettings = (params: ProjectIdParam) =>
  new URL("admin/", projectBase(params));

export const projectSettingsGeneral = (params: ProjectIdParam) =>
  new URL("general/", projectSettings(params));

export const projectSettingsEngines = (params: ProjectIdParam) =>
  new URL("engines/", projectSettings(params));

export const projectSettingsBiApplications = (params: ProjectIdParam) =>
  new URL("bi-applications/", projectSettings(params));

export const projectSettingsReflections = (params: ProjectIdParam) =>
  new URL("reflections/", projectSettings(params));

export const projectSettingsEngineRouting = (params: ProjectIdParam) =>
  new URL("engine-routing/", projectSettings(params));

export const projectSettingsSql = (params: ProjectIdParam) =>
  new URL("sql/", projectSettings(params));
