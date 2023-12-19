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

export type ListReflectionJobsParams = {
  reflectionId: string;
  sort: string;
  order: "ASCENDING" | "DESCENDING";
  filter?: string;
  limit?: number;
  offset?: number;
};

export const listReflectionJobsUrl = ({
  reflectionId,
  sort,
  order,
  offset,
  limit,
}: ListReflectionJobsParams) => {
  return new APIV2Call()
    .paths(`jobs/reflection/${reflectionId}`)
    .params(sort ? { sort: sort } : {})
    .params(order ? { order: order } : {})
    .params(offset ? { offset: offset } : {})
    .params(limit ? { limit: limit } : {})
    .toString();
};

export const listReflectionJobs = async (
  params: ListReflectionJobsParams
): Promise<any> => {
  return getApiContext()
    .fetch(listReflectionJobsUrl(params))
    .then((res: any) => res.json());
};
