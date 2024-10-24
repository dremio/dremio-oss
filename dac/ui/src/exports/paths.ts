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

import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

export const organization = () => `/organization` as const;

export type BranchId = string;
export type CommitId = string;
export type Namespace = string;
export type BranchIdParam = { branchId: BranchId };
export type CommitIdParam = { commitId: CommitId };
export type NamespaceParam = { namespace?: Namespace };

export type SourceId = string;
export type SourceIdParam = { sourceId: SourceId };
type ProjectIdParam = { projectId?: string };

/**
 * Sonar
 */

export type SonarProjectId = string;

export type SonarProjectIdParam = { projectId: SonarProjectId };

export type SonarSourceNameParam = SonarProjectIdParam & { sourceName: string };

const sonarBase = "/sonar";

/**
 * @deprecated import from sonarPaths
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const datasets = (params: SonarProjectIdParam) => "/" as const;

/**
 * @deprecated import from sonarPaths
 */

export const sources = (params: SonarSourceNameParam) =>
  `/source/${params.sourceName}` as const;

/**
 * @deprecated import from sonarPaths
 */
export const sonarProjects = () => `${sonarBase}` as const;
/**
 * @deprecated import from sonarPaths
 */
export const job = (params: { jobId: string }) =>
  `/jobs/job/${params.jobId}` as const;

/**
 * @deprecated import from sonarPaths
 */
export const jobsNew = () => `/jobs-new` as const;
/**
 * @deprecated import from sonarPaths
 */
export const newQuery = () => `/new_query` as const;

/**
 * @deprecated import from sonarPaths
 */
export const jobs = () => `/jobs` as const;
export const login = () => `/login` as const;

/**
 * Nessie Sources in Software
 */

export const sourceBase = (params?: ProjectIdParam) => {
  const projectId =
    params?.projectId || getSonarContext()?.getSelectedProjectId?.();
  return commonPaths.sources.link({
    projectId,
  });
};

const nessieBase = "/nessie";

export const nessieSourceBase = (params: SourceIdParam & ProjectIdParam) =>
  `${sourceBase({ projectId: params.projectId })}${nessieBase}/${
    params.sourceId
  }` as const;

export const nessieSourceCommitsBase = (
  params: SourceIdParam & ProjectIdParam,
) => `${nessieSourceBase(params)}/commits` as const;

export const nessieSourceCommitsNonBase = () => `commits` as const;
export const nessieSourceTagsNonBase = () => `tags` as const;
export const nessieSourceBranchesNonBase = () => `branches` as const;

export const nessieSourceCommits = (params: BranchIdParam & NamespaceParam) =>
  `${nessieSourceCommitsNonBase()}/${params.branchId}${
    params.namespace ? `/${params.namespace}` : ""
  }` as const;

export const nessieSourceCommit = (params: BranchIdParam & CommitIdParam) =>
  `commit/${params.branchId}/${params.commitId}` as const;
