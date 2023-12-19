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

import { JobsQueryParams } from "dremio-ui-common/types/Jobs.types";
import { parseQueryState } from "dremio-ui-common/utilities/jobs.js";
// @ts-ignore
import { JobStates } from "dremio-ui-common/sonar/components/JobStatus.js";
import {
  JOB_COLUMNS,
  //@ts-ignore
} from "dremio-ui-common/sonar/components/JobsTable/jobsPageTableColumns.js";
import { GenericFilters } from "./components/JobsFilters/utils";

type Query = {
  filters: any;
  sort: string;
  order: string;
};

const escapeFilterValue = (value: string) =>
  value.replace(/\\/, "\\\\").replace(/"/g, '\\"');

// copied from jobsQueryState.js, and reformatted for non-immutable
export const formatJobsBackendQuery = (query: JobsQueryParams) => {
  const filters = query.filters;
  const filterStrings = Object.entries(filters)
    .map(([key, values]) => {
      if (!values) {
        return null;
      }
      if (key === "st" && values instanceof Array) {
        //start time
        return `(st=gt=${values[0]};st=lt=${values[1]})`;
      } else if (key === "contains" && values instanceof Array) {
        return `*=contains="${escapeFilterValue(values?.[0])}"`;
      }
      if (values instanceof Array && values.length) {
        return (
          "(" +
          values
            .map((value) => `${key}=="${escapeFilterValue(value)}"`)
            .join(",") +
          ")"
        );
      }
    })
    .filter((x) => x);

  const sort = query.sort;
  const order = query.order;
  return {
    sort,
    order,
    filter: filterStrings.join(";"),
  };
};

export const getPageIndexOfCachedJob = (jobs: any[], jobId: string) => {
  const index = jobs.findIndex((job) => job.id === jobId);
  return index < 100 ? 1 : Math.ceil(index / 100);
};

export const JobSortColumns: any = {
  [JOB_COLUMNS.jobId]: "job",
  [JOB_COLUMNS.user]: "usr",
  [JOB_COLUMNS.duration]: "dur",
  [JOB_COLUMNS.startTime]: "st",
};

export const JobSortColumnsReverse: any = {
  job: JOB_COLUMNS.jobId,
  usr: JOB_COLUMNS.user,
  dur: JOB_COLUMNS.duration,
  st: JOB_COLUMNS.startTime,
};

export const formatJobQueryState = (query: JobsQueryParams) => {
  const { filters } = query;
  return {
    // add sort/order into URL when ready to be added
    filters: JSON.stringify(filters),
    sort: query.sort,
    order: query.order,
  };
};

export const DefaultJobQuery = {
  order: "DESCENDING",
  sort: "st",
  filters: '{"qt":["UI","EXTERNAL"]}',
};

export const isRunningJob = (state: any) =>
  ![JobStates.FAILED, JobStates.CANCELED, JobStates.COMPLETED].includes(state);

const UI_FILTERS = [
  GenericFilters.jst,
  GenericFilters.st,
  GenericFilters.qt,
  GenericFilters.usr,
  "contains",
];
// Prevent storage if directed from non-jobs page (use BE-driven filter params)
export const hasOnlyFrontendFilters = (query: Query) => {
  if (!query.filters) return false;

  const queryState = parseQueryState(query);
  const curFilters = Object.keys(queryState.filters);
  return (
    curFilters.filter((filter) => !UI_FILTERS.includes(filter)).length === 0
  );
};
