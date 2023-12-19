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

import { APIV2Call } from "@app/core/APICall";
// @ts-ignore
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";

export type ListJobsParams = {
  pageToken: {
    limit: number;
    offset?: number;
  };
  sort: string;
  order: "ASCENDING" | "DESCENDING";
  filter: string;
};

// TODO: Should use next-token param to set offset, limit, and level params
export const listJobsUrl = (params: ListJobsParams) => {
  const { sort, order, filter, pageToken } = params;
  return new APIV2Call()
    .paths("jobs-listing/v1.0")
    .params(pageToken.offset ? { offset: pageToken.offset } : {})
    .params(pageToken.offset ? { limit: pageToken.limit } : {})
    .params(pageToken.offset ? { level: 0 } : { detailLevel: 1 })
    .params(sort ? { sort: sort } : {})
    .params(order ? { order: order } : {})
    .params(filter ? { filter: filter } : {});
};

export const listJobs = async (params: ListJobsParams): Promise<any> => {
  return getApiContext()
    .fetch(listJobsUrl(params))
    .then((res: any) => res.json());
};
