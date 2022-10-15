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
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { renderQueryStateForServer } from "utils/jobsQueryState";

export const FETCH_JOBS_LIST_REQUEST = "FETCH_JOBS_LIST_REQUEST";
export const FETCH_JOBS_LIST_SUCCESS = "FETCH_JOBS_LIST_SUCCESS";
export const FETCH_JOBS_LIST_FAILURE = "FETCH_JOBS_LIST_FAILURE";
export const JOBS_LIST_RESET = "JOBS_LIST_RESET";
export const FETCH_JOB_DETAILS_BY_ID_REQUEST =
  "FETCH_JOB_DETAILS_BY_ID_REQUEST";
export const FETCH_JOB_DETAILS_BY_ID_SUCCESS =
  "FETCH_JOB_DETAILS_BY_ID_SUCCESS";
export const FETCH_JOB_DETAILS_BY_ID_FAILURE =
  "FETCH_JOB_DETAILS_BY_ID_FAILURE";
export const ITEMS_FOR_FILTER_JOBS_LIST_REQUEST =
  "ITEMS_FOR_FILTER_JOBS_LIST_REQUEST";
export const ITEMS_FOR_FILTER_JOBS_LIST_SUCCESS =
  "ITEMS_FOR_FILTER_JOBS_LIST_SUCCESS";
export const ITEMS_FOR_FILTER_JOBS_LIST_FAILURE =
  "ITEMS_FOR_FILTER_JOBS_LIST_FAILURE";
export const JOB_DETAILS_VIEW_ID = "JOB_DETAILS_VIEW_ID";
export const JOB_PAGE_NEW_VIEW_ID = "JOB_PAGE_NEW_VIEW_ID";
export const FETCH_JOB_EXECUTION_DETAILS_BY_ID_REQUEST =
  "FETCH_JOB_EXECUTION_DETAILS_BY_ID_REQUEST";
export const FETCH_JOB_EXECUTION_DETAILS_BY_ID_SUCCESS =
  "FETCH_JOB_EXECUTION_DETAILS_BY_ID_SUCCESS";
export const FETCH_JOB_EXECUTION_DETAILS_BY_ID_FAILURE =
  "FETCH_JOB_EXECUTION_DETAILS_BY_ID_FAILURE";
export const FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_REQUEST =
  "FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_REQUEST";
export const FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_SUCCESS =
  "FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_SUCCESS";
export const FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_FAILURE =
  "FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_FAILURE";

function fetchJobsListAction(queryState, viewId) {
  const meta = { viewId };
  const query = renderQueryStateForServer(queryState);
  return (dispatch) => {
    dispatch({ type: FETCH_JOBS_LIST_REQUEST, meta });
    return apiUtils
      .fetch(`jobs-listing/v1.0?detailLevel=1&${query}`, {}, 2)
      .then((response) => {
        try {
          return response.json();
        } catch {
          // eslint-disable-next-line promise/no-return-wrap
          return Promise.reject(response);
        }
      })
      .then((payload) => {
        dispatch(fetchJobsListActionSuccess(payload, meta));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((response) => {
        if (Object.prototype.isPrototypeOf.call(Response.prototype, response)) {
          response
            .json()
            .then((error) => {
              return dispatch(fetchJobsListActionFailure({ response: error }));
            })
            .catch(() => dispatch(fetchJobsListActionFailure(response)));
        } else {
          dispatch(
            fetchJobsListActionFailure({
              response,
              meta: { notification: true },
            })
          );
        }
      });
  };
}

export const resetFilteredJobsList = () => ({
  type: JOBS_LIST_RESET,
  payload: Immutable.List(),
});

function fetchFilteredJobsListForExplorePageAction(
  jobId = "",
  viewId,
  replaceIndex
) {
  const meta = { viewId, isExplorePage: true, replaceIndex };
  return (dispatch) => {
    return apiUtils
      .fetch(
        `jobs-listing/v1.0?detailLevel=1&sort=st&order=ASCENDING&filter=(${encodeURIComponent(
          `job=="${jobId}"`
        )
          .replace(/'/g, "%27")
          .replace(/"/g, "%22")})`,
        {},
        2
      )
      .then((response) => {
        try {
          return response.json();
        } catch {
          // eslint-disable-next-line promise/no-return-wrap
          return Promise.reject(response);
        }
      })
      .then((payload) => {
        dispatch(fetchJobsListActionSuccess(payload, meta));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((response) => {
        if (response) {
          return response
            .json()
            .then((error) => {
              return dispatch(fetchJobsListActionFailure({ response: error }));
            })
            .catch(
              () => response && dispatch(fetchJobsListActionFailure(response))
            );
        }
      });
  };
}

const fetchJobsListActionSuccess = (payload, meta) => ({
  type: FETCH_JOBS_LIST_SUCCESS,
  payload,
  meta,
});

const fetchJobsListActionFailure = (response) => ({
  type: FETCH_JOBS_LIST_FAILURE,
  response,
  meta: {
    notification: true,
    showDefaultMoreInfo: false,
  },
  error: true,
});

export function fetchJobsList(queryState, viewId) {
  return (dispatch) => {
    return dispatch(fetchJobsListAction(queryState, viewId));
  };
}

export function fetchFilteredJobsList(jobId, viewId, replaceIndex) {
  return (dispatch) => {
    return dispatch(
      fetchFilteredJobsListForExplorePageAction(jobId, viewId, replaceIndex)
    );
  };
}

function loadJobDetailsAction(
  jobId,
  viewId,
  attempts = 1,
  skipStartAction = false
) {
  const meta = { viewId };
  return (dispatch) => {
    if (!skipStartAction) {
      dispatch({ type: FETCH_JOB_DETAILS_BY_ID_REQUEST, meta });
    }
    const params = new URLSearchParams();
    params.append("detailLevel", 1);
    if (attempts !== undefined) {
      params.append("attempt", attempts);
    }
    return apiUtils
      .fetch(
        `jobs-listing/v1.0/${jobId}/jobDetails?${params.toString()}`,
        {},
        2
      )
      .then((response) => {
        try {
          return response.json();
        } catch {
          // eslint-disable-next-line promise/no-return-wrap
          return Promise.reject(response);
        }
      })
      .then((payload) => {
        dispatch(loadJobDetailsActionSuccess(payload, meta));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((response) => {
        const errorPayload = {
          meta: {
            notification: true,
            showDefaultMoreInfo: false,
          },
          error: true,
        };
        return response
          .json()
          .then((error) => {
            if (response.status === 404) {
              dispatch(loadJobDetailsActionFailure({ response: error }));
              // eslint-disable-next-line promise/no-return-wrap
              return Promise.resolve({ error, status: response.status });
            } else {
              dispatch(
                loadJobDetailsActionFailure({ response: error }, errorPayload)
              );
            }
            return null;
          })
          .catch(() => dispatch(loadJobDetailsActionFailure(response)));
      });
  };
}

const loadJobDetailsActionSuccess = (payload, meta) => ({
  type: FETCH_JOB_DETAILS_BY_ID_SUCCESS,
  payload,
  meta,
});

const loadJobDetailsActionFailure = (response, meta = {}) => ({
  type: FETCH_JOB_DETAILS_BY_ID_FAILURE,
  response,
  ...meta,
  error: true,
});

export function loadJobDetails(jobId, viewId, attempts, skipStartAction) {
  return (dispatch) => {
    return dispatch(
      loadJobDetailsAction(jobId, viewId, attempts, skipStartAction)
    );
  };
}

function fetchItemsForFilter(tag, filter = "", limit = "50") {
  return (dispatch) => {
    return apiUtils
      .fetch(`jobs/filters/${tag}?filter=${filter}&limit=${limit}`, {}, 2)
      .then((response) => {
        try {
          return response.json();
        } catch {
          // eslint-disable-next-line promise/no-return-wrap
          return Promise.reject(response);
        }
      })
      .then((payload) => {
        dispatch(fetchItemsForFilterSuccess(payload, tag));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((payload) =>
        dispatch(fetchItemsForFilterFailure(payload, { tag }))
      );
  };
}

const fetchItemsForFilterSuccess = (payload, tag) => ({
  type: ITEMS_FOR_FILTER_JOBS_LIST_SUCCESS,
  payload,
  meta: { tag },
});

const fetchItemsForFilterFailure = (payload, tag) => ({
  type: ITEMS_FOR_FILTER_JOBS_LIST_FAILURE,
  payload,
  meta: {
    ...tag,
  },
  error: true,
});

export function loadItemsForFilter(tag, filter, limit) {
  return (dispatch) => {
    return dispatch(fetchItemsForFilter(tag, filter, limit));
  };
}

export const SET_JOB_LIST_CLUSTER_TYPE = "SET_JOB_LIST_CLUSTER_TYPE";

export const setClusterType = (value) => ({
  type: SET_JOB_LIST_CLUSTER_TYPE,
  payload: value,
});

export const LOAD_NEXT_JOBS_LIST_REQUEST = "LOAD_NEXT_JOBS_LIST_REQUEST";
export const LOAD_NEXT_JOBS_LIST_SUCCESS = "LOAD_NEXT_JOBS_LIST_SUCCESS";
export const LOAD_NEXT_JOBS_LIST_FAILURE = "LOAD_NEXT_JOBS_LIST_FAILURE";

const fetchNextJobList = (href, viewId) => {
  const meta = { viewId };
  return (dispatch) => {
    dispatch({ type: LOAD_NEXT_JOBS_LIST_REQUEST, meta });
    return apiUtils
      .fetch(href.slice(6), {}, 2)
      .then((response) => {
        try {
          return response.json();
        } catch {
          // eslint-disable-next-line promise/no-return-wrap
          return Promise.reject(response);
        }
      })
      .then((payload) => {
        dispatch(fetchNextJobsSuccess(payload, meta));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((response) =>
        response
          .json()
          .then((error) => {
            return dispatch(fetchNextJobsFailure({ response: error }));
          })
          .catch(() => dispatch(fetchNextJobsFailure(response)))
      );
  };
};

const fetchNextJobsSuccess = (payload, meta) => ({
  type: LOAD_NEXT_JOBS_LIST_SUCCESS,
  payload,
  meta,
});

const fetchNextJobsFailure = (response) => ({
  type: LOAD_NEXT_JOBS_LIST_FAILURE,
  response,
  meta: {
    notification: true,
    showDefaultMoreInfo: false,
  },
  error: true,
});

export function loadNextJobs(href, viewId) {
  return (dispatch) => {
    return dispatch(fetchNextJobList(href, viewId));
  };
}

export const fetchJobExecutionDetails =
  (jobId, viewId, totalAttempts = 1, skipStartAction = false) =>
  async (dispatch) => {
    const meta = { viewId };
    if (!skipStartAction) {
      dispatch({ type: FETCH_JOB_EXECUTION_DETAILS_BY_ID_REQUEST, meta });
    }
    try {
      let response = await apiUtils.fetch(
        `queryProfile/${jobId}/JobProfile?attempt=${totalAttempts}`,
        {},
        2
      );
      response = await response.json();
      dispatch(fetchJobExecutionDetailsSuccess(response, meta));
    } catch (response) {
      const error = await response.json();
      const failureMeta = { ...meta, ...error };
      return dispatch(
        fetchJobExecutionDetailsFailure({ response: error }, failureMeta)
      );
    }
  };

const fetchJobExecutionDetailsSuccess = (payload, meta) => ({
  type: FETCH_JOB_EXECUTION_DETAILS_BY_ID_SUCCESS,
  payload,
  meta,
});

const fetchJobExecutionDetailsFailure = (payload, meta) => ({
  type: FETCH_JOB_EXECUTION_DETAILS_BY_ID_FAILURE,
  payload,
  meta: {
    ...meta,
    notification: true,
  },
  error: true,
});

export function fetchJobExecutionOperatorDetails(
  jobId,
  viewId,
  phaseId,
  operatorId,
  totalAttempts = 1
) {
  const meta = { viewId };
  return (dispatch) => {
    dispatch({
      type: FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_REQUEST,
      meta,
      payload: {},
    });
    return apiUtils
      .fetch(
        `queryProfile/${jobId}/JobProfile/OperatorDetails?attempt=${totalAttempts}&phaseId=${phaseId}&operatorId=${operatorId}`,
        {},
        2
      )
      .then((response) => response && response.json())
      .then((payload) => {
        dispatch(fetchJobExecutionOperatorDetailsSuccess(payload, meta));
        // eslint-disable-next-line promise/no-return-wrap
        return Promise.resolve(payload);
      })
      .catch((response) =>
        response
          .json()
          .then((error) => {
            return dispatch(
              fetchJobExecutionOperatorDetailsFailure(
                { response: { ...error, status: response.status } },
                meta
              )
            );
          })
          .catch(() =>
            dispatch(fetchJobExecutionOperatorDetailsFailure(response, meta))
          )
      );
  };
}

const fetchJobExecutionOperatorDetailsSuccess = (payload, meta) => ({
  type: FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_SUCCESS,
  payload,
  meta,
});

const fetchJobExecutionOperatorDetailsFailure = (payload, meta) => ({
  type: FETCH_JOB_EXECUTION_OPERATOR_DETAILS_BY_ID_FAILURE,
  payload,
  meta: {
    ...meta,
    hideError: true,
  },
  error: true,
});
