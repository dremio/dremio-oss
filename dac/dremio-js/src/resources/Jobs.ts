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

import type { SonarResourceConfig } from "../internal/Config.js";
import type { OffsetParams, LimitParams } from "../types/Params.js";
import { withOffsetAsyncIter } from "../internal/IterationHelpers.js";
import { Job } from "../classes/Job.js";
import { Query } from "../classes/Query.js";

type JobsSortable = "dur";

type JobsSortOptions =
  | {
    sort: JobsSortable;
    order: "asc" | "desc";
  }
  | {
    sort?: never;
    order?: never;
  };

export const Jobs = (config: SonarResourceConfig) => ({
  list: withOffsetAsyncIter(
    (
      params: Partial<OffsetParams & LimitParams> & {
        batch_size?: number;
      } & JobsSortOptions & { filter?: string; } = {},
    ) => {
      const search = new URLSearchParams({
        ...(params.limit && { limit: String(params.limit) }),
        ...(params.offset && { offset: String(params.offset) }),
        ...(params.sort && {
          sort: params.sort,
          order: params.order === "asc" ? "ASCENDING" : "DESCENDING",
        }),
        ...(params.filter && { filter: params.filter })
      });
      return config
        .sonarV2Request(`jobs-listing/v1.0?${search.toString()}`)
        .then(
          (res) =>
            res.json(),
        )
        .then((collection) => {
          return {
            data: collection.jobs.map(
              (jobDetails: any) => new Job(config, jobDetails),
            ),
            hasNextPage: !!collection.next?.length,
          };
        });
    },
  ),
  retrieve: (jobId: string) => Job.fromId(config, jobId),
  create: (query: Query) => Job.fromQuery(config, query),
});
