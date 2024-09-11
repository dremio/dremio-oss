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
import type { CatalogReference } from "./CatalogReference.js";

export type CommunitySourceProperties = {
  acceleration: {
    activePolicyType: string;
    gracePeriod: Temporal.Duration;
    neverExpire: boolean;
    neverRefresh: boolean;
    refreshPeriod: Temporal.Duration;
    refreshSchedule: string;
  };

  allowCrossSourceSelection: boolean;
  config: {
    accessKey: string;
    compatibilityMode: boolean;
    credentialType: string;
    defaultCtasFormat: string;
    enableAsync: boolean;
    enableFileStatusCheck: boolean;
    externalBucketList: string[];
    isCachingEnabled: boolean;
    isPartitionInferenceEnabled: boolean;
    maxCacheSpacePct: number;
    requesterPays: boolean;
    rootPath: string;
    secure: boolean;
  };
  createdAt: Date;
  disableMetadataValidityCheck: boolean;
  id: string;
  metadataPolicy: {
    authTTL: Temporal.Duration;
    autoPromoteDatasets: boolean;
    datasetExpireAfter: Temporal.Duration;
    datasetRefreshAfter: Temporal.Duration;
    datasetUpdateMode: string;
    deleteUnavailableDatasets: boolean;
    namesRefresh: Temporal.Duration;
  };
  name: string;
  status: string;
  type: string;
};

export type CommunitySourceMethods = {
  children(): {
    data(): AsyncGenerator<CatalogReference>;
  };
} & CatalogObjectMethods;

export type CommunitySource = CommunitySourceProperties &
  CommunitySourceMethods;
