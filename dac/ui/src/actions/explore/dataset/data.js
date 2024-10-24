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
import { RSAA } from "redux-api-middleware";

import apiUtils from "#oss/utils/apiUtils/apiUtils";
import { APIV2Call } from "#oss/core/APICall";

export const PAGE_SIZE = 100;

export const LOAD_NEXT_ROWS_START = "LOAD_NEXT_ROWS_START";
export const LOAD_NEXT_ROWS_SUCCESS = "LOAD_NEXT_ROWS_SUCCESS";
export const LOAD_NEXT_ROWS_FAILURE = "LOAD_NEXT_ROWS_FAILURE";

const fetchNextRows = (datasetVersion, paginationUrl, offset) => {
  const href = `${paginationUrl}?offset=${offset}&limit=${PAGE_SIZE}`;

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        LOAD_NEXT_ROWS_START,
        { type: LOAD_NEXT_ROWS_SUCCESS, meta: { datasetVersion, offset } },
        // empty meta to not break existing functionality
        { type: LOAD_NEXT_ROWS_FAILURE, meta: {} },
      ],
      method: "GET",
      headers: apiUtils.getJobDataNumbersAsStringsHeader(),
      endpoint: apiCall,
    },
  };
};

export const loadNextRows =
  (datasetVersion, paginationUrl, offset) => (dispatch) => {
    return dispatch(fetchNextRows(datasetVersion, paginationUrl, offset));
  };

export const FULL_CELL_VIEW_ID = "FULL_CELL_VIEW_ID";

export const LOAD_FULL_CELL_VALUE_START = "LOAD_FULL_CELL_VALUE_START";
export const LOAD_FULL_CELL_VALUE_SUCCESS = "LOAD_FULL_CELL_VALUE_SUCCESS";
export const LOAD_FULL_CELL_VALUE_FAILURE = "LOAD_FULL_CELL_VALUE_FAILURE";

export const CLEAR_FULL_CELL_VALUE = "CLEAR_FULL_CELL_VALUE";

export const loadFullCellValue = ({ href }) => {
  const meta = { viewId: FULL_CELL_VIEW_ID };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_FULL_CELL_VALUE_START, meta },
        { type: LOAD_FULL_CELL_VALUE_SUCCESS, meta },
        { type: LOAD_FULL_CELL_VALUE_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
};

export const clearFullCellValue = () => (dispatch) =>
  dispatch({ type: CLEAR_FULL_CELL_VALUE });

export const EXPLORE_PAGE_LISTENER_START = "EXPLORE_PAGE_LISTENER_START";
export const EXPLORE_PAGE_LISTENER_STOP = "EXPLORE_PAGE_LISTENER_STOP";
export const EXPLORE_PAGE_LOCATION_CHANGED = "EXPLORE_PAGE_LOCATION_CHANGED";
export const EXPLORE_PAGE_EXIT = "EXPLORE_PAGE_EXIT";
/**
 * Starts explore page change listener, that tries to load data for a current dataset
 * @param {bool} doInitialLoad - pass true if you like to try load data for current page
 */
export const startExplorePageListener = (doInitialLoad) => ({
  type: EXPLORE_PAGE_LISTENER_START,
  doInitialLoad,
});
/**
 * Stops explore page change listener
 */
export const stopExplorePageListener = () => ({
  type: EXPLORE_PAGE_LISTENER_STOP,
});

export const explorePageExit = () => ({ type: EXPLORE_PAGE_EXIT });

/**
 * Action that notifies that explore page location was changed and provides previous and new state of route.
 * For description {@see prevState} and {@see nextState} definition see react-router v3 api
 * {@link https://github.com/ReactTraining/react-router/blob/v3/docs/API.md#onchangeprevstate-nextstate-replace-callback}
 *
 * @param {object} newRouteState - nextState argument of route's onChange callback
 * @returns a redux action
 */
export const explorePageLocationChanged = (newRouteState) => ({
  type: EXPLORE_PAGE_LOCATION_CHANGED,
  newRouteState,
});

export const UPDATE_EXPLORE_JOB_PROGRESS = "UPDATE_EXPLORE_JOB_PROGRESS";
export const updateExploreJobProgress = (jobUpdate) => ({
  type: UPDATE_EXPLORE_JOB_PROGRESS,
  jobUpdate,
});

export const SET_EXPLORE_JOBID_IN_PROGRESS = "SET_EXPLORE_JOBID_IN_PROGRESS";
export const setExploreJobIdInProgress = (jobId, datasetVersion, tabId) => ({
  type: SET_EXPLORE_JOBID_IN_PROGRESS,
  jobId,
  datasetVersion,
  meta: {
    tabId,
  },
});

export const UPDATE_EXPLORE_JOB_RECORDS = "UPDATE_EXPLORE_JOB_RECORDS";
export const updateJobRecordCount = (recordCount, datasetVersion) => ({
  type: UPDATE_EXPLORE_JOB_RECORDS,
  recordCount,
  datasetVersion,
});

export const INIT_EXPLORE_JOB_PROGRESS = "INIT_EXPLORE_JOB_PROGRESS";
// initialize explore job progess
export const initializeExploreJobProgress = (isRun) => ({
  type: INIT_EXPLORE_JOB_PROGRESS,
  isRun,
});

export const FAILED_EXPLORE_JOB_PROGRESS = "FAILED_EXPLORE_JOB_PROGRESS";
export const failedExploreJobProgress = () => ({
  type: FAILED_EXPLORE_JOB_PROGRESS,
});
