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

import { Dispatch } from "redux";
import { getJobSummary } from "#oss/exports/endpoints/JobsListing/getJobSummary";
import { getJobDetails } from "#oss/exports/endpoints/JobsListing/getJobDetails";

export const FETCH_JOB_SUMMARY = "FETCH_JOB_SUMMARY";

export function fetchJobSummary({
  jobId,
  maxSqlLength,
  tabId,
}: {
  jobId: string;
  maxSqlLength?: number;
  tabId?: string;
}) {
  return async (dispatch: Dispatch) => {
    const summary = await getJobSummary(jobId, maxSqlLength);
    dispatch({ type: FETCH_JOB_SUMMARY, summary, tabId });
    return summary;
  };
}

export const FETCH_JOB_DETAILS = "FETCH_JOB_DETAILS";

export function fetchJobDetails({
  jobId,
  tabId,
}: {
  jobId: string;
  tabId?: string;
}) {
  return async (dispatch: Dispatch) => {
    const details = await getJobDetails(jobId);
    dispatch({ type: FETCH_JOB_DETAILS, details, tabId });
    return details;
  };
}

export const REMOVE_EXPLORE_JOB = "REMOVE_EXPLORE_JOB";

export function removeExploreJob(jobId: string) {
  return {
    type: REMOVE_EXPLORE_JOB,
    jobId,
  };
}

export const CLEAR_EXPLORE_JOBS = "CLEAR_EXPLORE_JOBS";

export function clearExploreJobs() {
  return {
    type: CLEAR_EXPLORE_JOBS,
  };
}
