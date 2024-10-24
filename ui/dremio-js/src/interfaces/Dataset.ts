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

import type { CatalogObjectMethods } from "./CatalogObject.js";
import type { Grantee } from "./Grantee.js";

export type CommunityDatasetProperties = {
  readonly createdAt: Date;
  /**
   * @deprecated
   */
  readonly id: string;
  readonly fields: { name: string; type: { name: string } }[];
  readonly owner: { id: string; type: "ROLE" | "USER" } | undefined;
  readonly path: string[];
  readonly type: `DATASET_${"DIRECT" | "PROMOTED" | "VIRTUAL"}`;
};

export type CommunityDatasetMethods = CatalogObjectMethods;

export type CommunityDataset = CommunityDatasetProperties &
  CommunityDatasetMethods;

export type EnterpriseDatasetMethods = {
  grants(): Promise<{
    availablePrivileges: string[];
    grants: { grantee: Grantee; privileges: string[] }[];
  }>;
};

export type EnterpriseDataset = CommunityDataset & EnterpriseDatasetMethods;
