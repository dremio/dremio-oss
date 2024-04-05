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

import {
  CLEAR_EXPLORE_JOBS,
  FETCH_JOB_DETAILS,
  FETCH_JOB_SUMMARY,
  REMOVE_EXPLORE_JOB,
} from "@app/actions/explore/exploreJobs";
import { UPDATE_JOB_STATE } from "@app/actions/jobs/jobs";
import { JobDetails } from "@app/exports/types/JobDetails.type";
import { JobSummary } from "@app/exports/types/JobSummary.type";

export type ExploreJobsState = {
  jobDetails: Partial<{ [key: string]: JobDetails }>;
  jobSummaries: Partial<{ [key: string]: JobSummary }>;
};

export const initialExploreJobsState: ExploreJobsState = {
  jobDetails: {},
  jobSummaries: {},
};

type ExploreJobsActionTypes =
  | {
      type: typeof FETCH_JOB_SUMMARY;
      summary: JobSummary;
    }
  | {
      type: typeof FETCH_JOB_DETAILS;
      details: JobDetails;
    }
  | {
      type: typeof UPDATE_JOB_STATE;
      jobId: string;
      payload: JobSummary;
    }
  | {
      type: typeof REMOVE_EXPLORE_JOB;
      jobId: string;
    }
  | {
      type: typeof CLEAR_EXPLORE_JOBS;
    };

export default function exploreJobs(
  state = initialExploreJobsState,
  action: ExploreJobsActionTypes,
): ExploreJobsState {
  switch (action.type) {
    case FETCH_JOB_SUMMARY:
      return {
        ...state,
        jobSummaries: {
          ...state.jobSummaries,
          [action.summary.id]: action.summary,
        },
      };

    case FETCH_JOB_DETAILS:
      return {
        ...state,
        jobDetails: {
          ...state.jobDetails,
          [action.details.id]: action.details,
        },
      };

    // used in /summary polling
    case UPDATE_JOB_STATE:
      return {
        ...state,
        jobSummaries: {
          ...state.jobSummaries,
          [action.jobId]: action.payload,
        },
      };

    case REMOVE_EXPLORE_JOB: {
      const nextState = { ...state };
      delete nextState.jobDetails[action.jobId];
      delete nextState.jobSummaries[action.jobId];
      return nextState;
    }

    case CLEAR_EXPLORE_JOBS:
      return initialExploreJobsState;

    default:
      return state;
  }
}
