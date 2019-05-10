/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { CALL_API } from 'redux-api-middleware';
import { replace, push } from 'react-router-redux';
import Immutable from 'immutable';
import { normalize } from 'normalizr';

import { API_URL_V2 } from 'constants/Api';
import param from 'jquery-param';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import jobDetailsSchema from 'schemas/jobDetails';
import { renderQueryState, renderQueryStateForServer } from 'utils/jobsQueryState';
import { addNotification } from 'actions/notification';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import { addParameterToUrl } from '@app/utils/urlUtils';

export const UPDATE_JOB_DETAILS = 'UPDATE_JOB_DETAILS';

export const updateJobDetails = (jobDetails) => ({
  type: UPDATE_JOB_DETAILS,
  payload: Immutable.fromJS(normalize(jobDetails, jobDetailsSchema))
});

export const UPDATE_JOB_STATE = 'UPDATE_JOB_STATE';

export const updateJobState = (jobId, payload) => ({
  type: UPDATE_JOB_STATE, jobId, payload
});

export const updateQueryState = (queryState) => {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(replace({...location, query: renderQueryState(queryState)}));
  };
};

export const FILTER_JOBS_REQUEST = 'FILTER_JOBS_REQUEST';
export const FILTER_JOBS_SUCCESS = 'FILTER_JOBS_SUCCESS';
export const FILTER_JOBS_FAILURE = 'FILTER_JOBS_FAILURE';

function filterJobsAction(queryState, viewId) {
  const meta = { viewId };
  const query = renderQueryStateForServer(queryState);
  const href = `/jobs/?${query}`;

  return {
    [CALL_API]:{
      types: [
        {type: FILTER_JOBS_REQUEST, meta},
        {type: FILTER_JOBS_SUCCESS, meta},
        {type: FILTER_JOBS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function filterJobsData(queryState, viewId) {
  return (dispatch) => {
    return dispatch(filterJobsAction(queryState, viewId));
  };
}

export const LOAD_NEXT_JOBS_REQUEST = 'LOAD_NEXT_JOBS_REQUEST';
export const LOAD_NEXT_JOBS_SUCCESS = 'LOAD_NEXT_JOBS_SUCCESS';
export const LOAD_NEXT_JOBS_FAILURE = 'LOAD_NEXT_JOBS_FAILURE';

function fetchNextJobs(href) {
  return {
    [CALL_API]:{
      types: [
        LOAD_NEXT_JOBS_REQUEST,
        LOAD_NEXT_JOBS_SUCCESS,
        LOAD_NEXT_JOBS_FAILURE
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function loadNextJobs(href) {
  return (dispatch) => {
    return dispatch(fetchNextJobs(href));
  };
}

export const ITEMS_FOR_FILTER_JOBS_REQUEST = 'ITEMS_FOR_FILTER_JOBS_REQUEST';
export const ITEMS_FOR_FILTER_JOBS_SUCCESS = 'ITEMS_FOR_FILTER_JOBS_SUCCESS';
export const ITEMS_FOR_FILTER_JOBS_FAILURE = 'ITEMS_FOR_FILTER_JOBS_FAILURE';

function fetchItemsForFilter(tag, filter = '', limit = '') {
  let params = '';
  if (filter || limit) {
    params = param({filter, limit});
  }
  return {
    [CALL_API]:{
      types: [
        ITEMS_FOR_FILTER_JOBS_REQUEST,
        {
          type: ITEMS_FOR_FILTER_JOBS_SUCCESS,
          meta: {tag}
        },
        ITEMS_FOR_FILTER_JOBS_FAILURE
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/jobs/filters/${tag}${params}`
    }
  };
}

export function loadItemsForFilter(tag, item, limit) {
  return (dispatch) => {
    return dispatch(fetchItemsForFilter(tag, item, limit));
  };
}

export const JOB_DETAILS_REQUEST = 'JOB_DETAILS_REQUEST';
export const JOB_DETAILS_SUCCESS = 'JOB_DETAILS_SUCCESS';
export const JOB_DETAILS_FAILURE = 'JOB_DETAILS_FAILURE';

function fetchJobDetails(jobId, viewId) {
  const meta = { jobId, viewId };
  return {
    [CALL_API]:{
      types: [
        {type: JOB_DETAILS_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(JOB_DETAILS_SUCCESS, jobDetailsSchema, meta),
        {type: JOB_DETAILS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/job/${jobId}/details`
    }
  };
}

export function loadJobDetails(jobId, viewId) {
  return (dispatch) => {
    return dispatch(fetchJobDetails(jobId, viewId));
  };
}

export const CANCEL_JOB_REQUEST = 'CANCEL_JOB_REQUEST';
export const CANCEL_JOB_SUCCESS = 'CANCEL_JOB_SUCCESS';
export const CANCEL_JOB_FAILURE = 'CANCEL_JOB_FAILURE';

const cancelJob = (jobId) => ({
  [CALL_API]: {
    types: [
      CANCEL_JOB_REQUEST,
      CANCEL_JOB_SUCCESS,
      CANCEL_JOB_FAILURE
    ],
    method: 'POST',
    endpoint: `${API_URL_V2}/job/${jobId}/cancel`
  }
});

export const cancelJobAndShowNotification = (jobId) => (dispatch) => {
  return dispatch(cancelJob(jobId))
    .then(action => dispatch(addNotification(action.payload.message, 'success' )));
};


export const ASK_GNARLY_STARTED = 'ASK_GNARLY_STARTED';
export const ASK_GNARLY_SUCCESS = 'ASK_GNARLY_SUCCESS';
export const ASK_GNARLY_FAILURE = 'ASK_GNARLY_FAILURE';

function fetchAskGnarly(jobId) {
  const href = '/support/' + jobId;
  return {
    [CALL_API]:{
      types: [
        {
          type: ASK_GNARLY_STARTED,
          meta: {
            notification: {
              message: la('Uploading data. Chat will open shortly.'),
              level: 'success'
            }
          }
        },
        {
          type: ASK_GNARLY_SUCCESS,
          meta: {
            notification: (payload) => {
              if (!payload.success) {
                return {
                  message: payload.url || la('Upload failed. Please check logs for details.'),
                  level: 'error'
                };
              }
              return {
                message: false
              };
            }
          }
        },
        {
          type: ASK_GNARLY_FAILURE,
          meta: {
            notification: {
              message: la('There was an error uploading. Please check logs for details.'),
              level: 'error'
            }
          }
        }
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function askGnarly(jobId) {
  return (dispatch) => {
    return dispatch(fetchAskGnarly(jobId));
  };
}

export function showJobProfile(profileUrl) {
  return (dispatch, getState) => {
    const location = getState().routing.locationBeforeTransitions;
    return dispatch(
      push({...location, state: {
        modal: 'JobProfileModal',
        profileUrl: `${API_URL_V2}${addParameterToUrl(profileUrl, 'Authorization',
          localStorageUtils.getAuthToken())}`
      }})
    );
  };
}
