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

export type AboutModalState = {
  activeTab: "build" | "metrics";
  alreadyFetchedBuildInfo: boolean;
  alreadyFetchedMetrics: boolean;
  metricsInProgress: boolean;
  users: number[];
  jobs: number[];
  average: { users: number; jobs: number };
};

export const initialState: AboutModalState = {
  activeTab: "build",
  alreadyFetchedBuildInfo: false,
  alreadyFetchedMetrics: false,
  metricsInProgress: false,
  users: [],
  jobs: [],
  average: {
    users: 0,
    jobs: 0,
  },
};

export type ActionTypes =
  | { type: "SET_ACTIVE_TAB"; activeTab: AboutModalState["activeTab"] }
  | {
      type: "SET_ALREADY_FETCHED_BUILD_INFO";
      alreadyFetchedBuildInfo: AboutModalState["alreadyFetchedBuildInfo"];
    }
  | {
      type: "SET_ALREADY_FETCHED_METRICS";
      alreadyFetchedMetrics: AboutModalState["alreadyFetchedMetrics"];
    }
  | {
      type: "SET_METRICS_IN_PROGRESS";
      metricsInProgress: AboutModalState["metricsInProgress"];
    }
  | {
      type: "SET_METRICS_CALCULATION_COMPLETE";
      users: AboutModalState["users"];
      jobs: AboutModalState["jobs"];
      average: AboutModalState["average"];
    };

export function AboutModalReducer(
  state: AboutModalState,
  action: ActionTypes,
): AboutModalState {
  switch (action.type) {
    case "SET_ACTIVE_TAB":
      return { ...state, activeTab: action.activeTab };
    case "SET_ALREADY_FETCHED_BUILD_INFO":
      return {
        ...state,
        alreadyFetchedBuildInfo: action.alreadyFetchedBuildInfo,
      };
    case "SET_ALREADY_FETCHED_METRICS":
      return { ...state, alreadyFetchedMetrics: action.alreadyFetchedMetrics };
    case "SET_METRICS_IN_PROGRESS":
      return { ...state, metricsInProgress: action.metricsInProgress };
    case "SET_METRICS_CALCULATION_COMPLETE":
      return {
        ...state,
        metricsInProgress: false,
        users: action.users,
        jobs: action.jobs,
        average: action.average,
      };
    default:
      return state;
  }
}
