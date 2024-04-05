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
import Immutable from "immutable";
import { getExploreState } from "./explore";

const getJobsList = (state) => state.jobs.jobs.get("jobs") || Immutable.List();
const getDataWithItemsForFiltersMap = (state) =>
  state.jobs.jobs.get("dataForFilter") || Immutable.Map();
const getImmutableJobList = (state) =>
  state.jobs.jobs.get("jobList") || Immutable.List();
const getDataWithItemsForJobListFiltersMap = (state) =>
  state.jobList.jobList.get("dataForFilter") || Immutable.Map();
const getUpdatedQueryStatuses = (state) =>
  getExploreState(state).view.queryStatuses || []; // TODO TABS: Pass in actual tabId

export const getJobs = createSelector([getJobsList], (jobs) => {
  return jobs;
});

export const getDataWithItemsForFilters = createSelector(
  [getDataWithItemsForFiltersMap],
  (filtersData) => {
    return filtersData;
  },
);

export const getJobList = createSelector([getImmutableJobList], (jobList) => {
  return jobList;
});

export const getQueryStatuses = createSelector(
  [getUpdatedQueryStatuses],
  (queryStatuses) => {
    return queryStatuses;
  },
);

export const getDataWithItemsForJobListFilters = createSelector(
  [getDataWithItemsForJobListFiltersMap],
  (filtersData) => {
    return filtersData;
  },
);
