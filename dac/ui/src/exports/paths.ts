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

export const organization = () => `/organization` as const;

/**
 * Arctic Catalog
 */

export type ArcticCatalogId = string;
export type BranchId = string;
export type CommitId = string;
export type Namespace = string;
export type BranchIdParam = { branchId: BranchId };
export type CatalogIdParam = { arcticCatalogId: ArcticCatalogId };
export type CommitIdParam = { commitId: CommitId };
export type NamespaceParam = { namespace?: Namespace };

const arcticBase = "/arctic";

export const arcticCatalogBase = (params: CatalogIdParam) =>
  `${arcticBase}/${params.arcticCatalogId}` as const;

export const arcticCatalog = (params: CatalogIdParam) =>
  `${arcticCatalogBase(params)}` as const;

export const arcticCatalogBranches = (params: CatalogIdParam) =>
  `${arcticCatalog(params)}/branches` as const;

export const arcticCatalogCommitsBase = (params: CatalogIdParam) =>
  `${arcticCatalog(params)}/commits` as const;

export const arcticCatalogCommits = (
  params: BranchIdParam & CatalogIdParam & NamespaceParam
) =>
  `${arcticCatalogCommitsBase(params)}/${params.branchId}${
    params.namespace ? `/${params.namespace}` : ""
  }` as const;

export const arcticCatalogCommit = (
  params: BranchIdParam & CatalogIdParam & CommitIdParam
) =>
  `${arcticCatalog(params)}/commit/${params.branchId}/${
    params.commitId
  }` as const;

export const arcticCatalogDataBase = (params: CatalogIdParam) =>
  `${arcticCatalog(params)}/data` as const;

export const arcticCatalogData = (
  params: BranchIdParam & CatalogIdParam & NamespaceParam
) =>
  `${arcticCatalogDataBase(params)}/${params.branchId}${
    params.namespace ? `/${params.namespace}` : ""
  }` as const;

export const arcticCatalogSettings = (params: CatalogIdParam) =>
  `${arcticCatalog(params)}/settings` as const;

export const arcticCatalogTags = (params: CatalogIdParam) =>
  `${arcticCatalog(params)}/tags` as const;

export const arcticCatalogs = () => `${arcticBase}` as const;

/**
 * Arctic Source
 */

export type SourceId = string;
export type SourceIdParam = { sourceId: SourceId };

const sourceBase = "/sources";

export const arcticSourceBase = (params: SourceIdParam) =>
  `${sourceBase}${arcticBase}/${params.sourceId}` as const;

export const arcticSourceCommitsBase = (params: SourceIdParam) =>
  `${arcticSourceBase(params)}/commits` as const;

export const arcticSourceCommitsNonBase = () => `commits` as const;
export const arcticSourceTagsNonBase = () => `tags` as const;
export const arcticSourceBranchesNonBase = () => `branches` as const;

export const arcticSourceCommits = (params: BranchIdParam & NamespaceParam) =>
  `${arcticSourceCommitsNonBase()}/${params.branchId}${
    params.namespace ? `/${params.namespace}` : ""
  }` as const;

export const arcticSourceCommit = (params: BranchIdParam & CommitIdParam) =>
  `commit/${params.branchId}/${params.commitId}` as const;

/**
 * Sonar
 */

export type SonarProjectId = string;

export type SonarProjectIdParam = { projectId: SonarProjectId };

const sonarBase = "/sonar";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const datasets = (params: SonarProjectIdParam) => "/" as const;
export const sonarProjects = () => `${sonarBase}` as const;

export const job = (params: { jobId: string }) =>
  `/jobs/job/${params.jobId}` as const;

export const jobsNew = () => `/jobs-new` as const;
export const newQuery = () => `/new_query` as const;

export const jobs = () => `/jobs` as const;
export const login = () => `/login` as const;
