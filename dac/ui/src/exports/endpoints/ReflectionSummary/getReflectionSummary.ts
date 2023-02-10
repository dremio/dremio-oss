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
  ReflectionSummary,
  ReflectionSummaryParams,
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
  sort?: [columnId: string, direction: string];
  pageSize: number;
  pageToken?: string;
};

const getFilterConfig = (params: Params) => {
  //@ts-ignore
  if (!params || params?.filter) return params;

  const availabilityStatus = params.availabilityStatus
    .map((x) => availabilityStatusMatrix[x].availabilityStatus)
    .filter(Boolean);
  const configStatus = params.availabilityStatus
    .map((x) => availabilityStatusMatrix[x].configStatus)
    .filter(Boolean);
  const enabledFlag = !params.availabilityStatus.some(
    (x) => availabilityStatusMatrix[x].enabledFlag === false
  );
  return {
    maxResults: params.pageSize,
    ...(params.pageToken && { pageToken: params.pageToken }),
    filter: {
      ...(params.reflectionNameOrDatasetPath && {
        reflectionNameOrDatasetPath: params.reflectionNameOrDatasetPath,
      }),
      ...(availabilityStatus.length && { availabilityStatus }),
      ...(configStatus.length && { configStatus }),
      ...(params.reflectionType && { reflectionType: params.reflectionType }),
      ...(!enabledFlag && { enabledFlag: false }),
      ...(params.refreshStatus?.length && {
        refreshStatus: params.refreshStatus,
      }),
    },
    ...(params.sort && { orderBy: getSortString(...params.sort) }),
  };
};

export const getReflectionSummary = async (
  params: Params
): Promise<ReflectionSummary> => {
  return getApiContext()
    .fetch(reflectionSummaryUrl(getFilterConfig(params)))
    .then((res) => res.json());
};
