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

import parseMilliseconds from "parse-ms";
import type {
  ResourceConfig,
  SonarV3Config,
} from "../../_internal/types/Config.js";
import { Reflection } from "./Reflection.js";
import type {
  ReflectionInterface,
  ReflectionProperties,
} from "../../interfaces/Reflection.js";
import type { SignalParam } from "../../_internal/types/Params.js";
import { DatasetCatalogReference } from "../catalog/CatalogReference.js";

const reflectionEntityToProperties = (properties: any): Reflection => {
  return new Reflection({
    createdAt: new Date(properties.createdAt),
    dataset: new DatasetCatalogReference({
      id: properties.datasetId,
      path: properties.datasetPath,
      type: properties.datasetType,
    }),
    id: properties.id,
    isArrowCachingEnabled: properties.isArrowCachingEnabled,
    isCanAlter: properties.isCanAlter,
    isCanView: properties.isCanView,
    metrics: {
      chosenCount: properties.chosenCount,
      consideredCount: properties.consideredCount,
      currentSizeBytes: properties.currentSizeBytes,
      failureCount: properties.status.failureCount,
      matchedCount: properties.matchedCount,
      outputRecords: properties.outputRecords,
      totalSizeBytes: properties.totalSizeBytes,
    },
    name: properties.name,
    status: {
      availabilityStatus: properties.status.availabilityStatus,
      combinedStatus: properties.status.combinedStatus,
      configStatus: properties.status.configStatus,
      expiresAt: new Date(properties.status.expiresAt),
      isEnabled: properties.isEnabled,
      lastDataFetchAt: properties.status.lastDataFetchAt
        ? new Date(properties.status.lastDataFetchAt)
        : null,
      lastFailureMessage: properties.status.lastFailureMessage || null,
      lastRefreshDuration:
        typeof properties.status.lastRefreshDurationMillis === "number"
          ? Temporal.Duration.from(
              parseMilliseconds(properties.status.lastRefreshDurationMillis),
            )
          : null,
      refreshMethod: properties.status.refreshMethod,
      refreshStatus: properties.status.refreshStatus,
    },
    type: properties.reflectionType,
    updatedAt: new Date(properties.updatedAt),
  } satisfies ReflectionProperties);
};

type ListParams = {
  limit: number;
  orderBy?: "datasetName" | "reflectionName" | "reflectionType";
};

type ListFilterParams = {
  availabilityStatus?: Set<ReflectionInterface["status"]["availabilityStatus"]>;
  refreshStatus?: Set<ReflectionInterface["status"]["refreshStatus"]>;
  type?: Set<ReflectionInterface["type"]>;
  nextPageToken?: never;
};

type PageTokenParams = {
  nextPageToken: string;
};

export const ReflectionsResource = (config: ResourceConfig & SonarV3Config) => {
  const getPage = (
    params: ListParams & (ListFilterParams | PageTokenParams) & SignalParam,
  ) => {
    const searchParams = new URLSearchParams();
    searchParams.set("maxResults", String(params.limit));

    if (!params.nextPageToken) {
      searchParams.set(
        "filter",
        encodeURIComponent(
          JSON.stringify({
            //@ts-ignore
            ...(params.availabilityStatus && {
              //@ts-ignore
              availabilityStatus: Array.from(params.availabilityStatus),
            }),
            //@ts-ignore
            ...(params.refreshStatus && {
              //@ts-ignore
              refreshStatus: Array.from(params.refreshStatus),
            }),
            //@ts-ignore
            ...(params.type && {
              //@ts-ignore
              reflectionType: Array.from(params.type),
            }),
          }),
        ),
      );
    }

    if (params.nextPageToken) {
      searchParams.set("pageToken", params.nextPageToken);
    }
    return config
      .sonarV3Request(`reflection-summary?${searchParams.toString()}`, {
        signal: params.signal,
      })
      .then((res) => res.json())
      .then((response) => {
        return {
          data: response.data.map((entity: any) =>
            reflectionEntityToProperties(entity),
          ),
          nextPageToken: (response.nextPageToken as string) || null,
        };
      });
  };

  return {
    _createFromEntity: reflectionEntityToProperties,
    list: (listParams: ListParams) => {
      return {
        getPage: (params: ListFilterParams | PageTokenParams | SignalParam) =>
          getPage({ ...listParams, ...params }),
      };
    },
  };
};
