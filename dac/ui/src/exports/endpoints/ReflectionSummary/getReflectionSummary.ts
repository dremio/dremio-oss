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

import { APIV3Call } from "@app/core/APICall";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import {
  type ReflectionSummaries,
  type ReflectionSummaryParams,
} from "dremio-ui-common/sonar/types/ReflectionSummary.type.js";

const reflectionSummaryUrl = (params: ReflectionSummaryParams) =>
  new APIV3Call().paths("/reflection-summary").params(params).toString();

const getSortString = (id, direction) => {
  return `${direction === "ascending" ? "" : "-"}${id}`;
};

const availabilityStatusMatrix = {
  AVAILABLE: {
    availabilityStatus: "AVAILABLE",
  },
  DISABLED: {
    enabledFlag: false,
  },
  EXPIRED: {
    availabilityStatus: "EXPIRED",
  },
  INVALID: {
    configStatus: "INVALID",
  },
  NONE: {
    availabilityStatus: "NONE",
  },
};

type Params = {
  availabilityStatus?: (keyof typeof availabilityStatusMatrix)[];
  reflectionType?: ("RAW" | "AGGREGATION")[];
  refreshStatus?: [];
  reflectionNameOrDatasetPath?: string;
  reflectionIds?: string[];
  sort?: [columnId: string, direction: string];
  pageSize: number;
  pageToken?: string;
};

const getFilterConfig = (params: Params) => {
  const availabilityStatus = (params.availabilityStatus || [])
    .map((x) => availabilityStatusMatrix[x].availabilityStatus)
    .filter(Boolean);
  const configStatus = (params.availabilityStatus || [])
    .map((x) => availabilityStatusMatrix[x].configStatus)
    .filter(Boolean);
  const enabledFlag = !(params.availabilityStatus || []).some(
    (x) => availabilityStatusMatrix[x].enabledFlag === false
  );
  const filter = {
    ...(params.reflectionNameOrDatasetPath && {
      reflectionNameOrDatasetPath: encodeURIComponent(
        params.reflectionNameOrDatasetPath
      ),
    }),
    ...(availabilityStatus.length && { availabilityStatus }),
    ...(configStatus.length && { configStatus }),
    ...(params.reflectionType && { reflectionType: params.reflectionType }),
    ...(!enabledFlag && { enabledFlag: false }),
    ...(params.refreshStatus?.length && {
      refreshStatus: params.refreshStatus,
    }),
    reflectionIds: params.reflectionIds,
  };
  return {
    ...(params.pageSize && { maxResults: params.pageSize }),
    ...(params.pageToken && { pageToken: params.pageToken }),
    filter: getApiContext().doubleEncodeJsonParam
      ? encodeURIComponent(JSON.stringify(filter))
      : filter,
    ...(params.sort && { orderBy: getSortString(...params.sort) }),
  };
};

const extractJobLinkFilters = (
  reflectionSummaries: ReflectionSummaries
): ReflectionSummaries => {
  const result = {
    ...reflectionSummaries,
    data: reflectionSummaries.data.map((reflectionSummary) => {
      return {
        ...reflectionSummary,
        chosenJobsFilters: reflectionSummary.chosenJobsLink
          ? JSON.parse(
              decodeURIComponent(
                reflectionSummary.chosenJobsLink.replace("/jobs?filters=", "")
              )
            )
          : null,
        consideredJobsFilters: reflectionSummary.consideredJobsLink
          ? JSON.parse(
              decodeURIComponent(
                reflectionSummary.consideredJobsLink.replace(
                  "/jobs?filters=",
                  ""
                )
              )
            )
          : null,
        matchedJobsFilters: reflectionSummary.matchedJobsLink
          ? JSON.parse(
              decodeURIComponent(
                reflectionSummary.matchedJobsLink.replace("/jobs?filters=", "")
              )
            )
          : null,
      };
    }),
  };
  return result;
};

export const getReflectionSummary = async (
  params: Params
): Promise<ReflectionSummaries> => {
  return getApiContext()
    .fetch(reflectionSummaryUrl(getFilterConfig(params)))
    .then((res) => res.json())
    .then((reflectionSummaries: ReflectionSummaries) =>
      extractJobLinkFilters(reflectionSummaries)
    );
};
