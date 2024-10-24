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

import { createSelector } from "reselect";
import {
  getDatasetVersionFromLocation,
  getExploreState,
  getFullDataset,
} from "#oss/selectors/explore";
import { getLocation } from "#oss/selectors/routing";
import { ExploreJobsState } from "#oss/reducers/explore/exploreJobs";
import { JobSummary } from "#oss/exports/types/JobSummary.type";

const getAllJobSummaries = (state: Record<string, any>) =>
  getExploreState(state)?.exploreJobs.jobSummaries || {};

export const getJobSummaries = createSelector(
  // @ts-expect-error Selector type mismatch
  [getAllJobSummaries],
  (jobSummaries: ExploreJobsState["jobSummaries"]) => {
    return jobSummaries;
  },
);

const getAllJobDetailsSelector = (state: Record<string, any>) =>
  getExploreState(state)?.exploreJobs.jobDetails || {};

export const getAllJobDetails = createSelector(
  // @ts-expect-error Selector type mismatch
  [getAllJobDetailsSelector],
  (allJobDetails: ExploreJobsState["jobDetails"]) => {
    return allJobDetails;
  },
);

export function getExploreJobId(state: Record<string, any>) {
  const location = getLocation(state);
  const version = getDatasetVersionFromLocation(location);
  const fullDataset = getFullDataset(state, version);
  const jobIdFromDataset: string | undefined = fullDataset?.getIn([
    "jobId",
    "id",
  ]);

  // a dataset is not returned in the first response of the new_tmp_untitled_sql endpoints
  // so we need to get the jobId from the most recently submitted job found in job summaries
  const jobSummaries = getJobSummaries(state);

  let mostRecentSummary: JobSummary | undefined;

  // JS objects do not guarantee insertion order, so we need to find a running job
  // or compare the end times and take the largest
  if (Object.values(jobSummaries).length) {
    mostRecentSummary = (Object.values(jobSummaries) as JobSummary[]).reduce(
      (previous, current) => {
        // since only one job can run at a time, an incomplete job must be the most recently submitted
        if (!current.isComplete) {
          return current;
        }

        if (!previous.isComplete) {
          return previous;
        }

        // return the job that completed last
        return previous.endTime > current.endTime ? previous : current;
      },
    );
  }

  return mostRecentSummary?.id && jobIdFromDataset !== mostRecentSummary.id
    ? mostRecentSummary.id
    : jobIdFromDataset;
}
