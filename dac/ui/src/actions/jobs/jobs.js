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
import { RSAA } from 'redux-api-middleware';
import { push, replace } from 'react-router-redux';
import Immutable from 'immutable';
import { normalize } from 'normalizr';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import jobDetailsSchema from 'schemas/jobDetails';
import { renderQueryState, renderQueryStateForServer } from 'utils/jobsQueryState';
import { addNotification } from 'actions/notification';
import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';
import { APIV2Call } from '@app/core/APICall';

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

  const apiCall = new APIV2Call().fullpath(`jobs/?${query}`);

  return {
    [RSAA]:{
      types: [
        {type: FILTER_JOBS_REQUEST, meta},
        {type: FILTER_JOBS_SUCCESS, meta},
        {type: FILTER_JOBS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
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
  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]:{
      types: [
        LOAD_NEXT_JOBS_REQUEST,
        LOAD_NEXT_JOBS_SUCCESS,
        LOAD_NEXT_JOBS_FAILURE
      ],
      method: 'GET',
      endpoint: apiCall
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
  const apiCall = new APIV2Call()
    .paths(`jobs/filters/${tag}`)
    .params({filter, limit});

  return {
    [RSAA]:{
      types: [
        ITEMS_FOR_FILTER_JOBS_REQUEST,
        {
          type: ITEMS_FOR_FILTER_JOBS_SUCCESS,
          meta: {tag}
        },
        ITEMS_FOR_FILTER_JOBS_FAILURE
      ],
      method: 'GET',
      endpoint: apiCall
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

export function loadJobDetails(jobId, viewId) {
  const meta = { jobId, viewId };

  const apiCall = new APIV2Call()
    .path('job')
    .path(jobId)
    .path('details');

  return {
    [RSAA]:{
      types: [
        {type: JOB_DETAILS_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(JOB_DETAILS_SUCCESS, jobDetailsSchema, meta),
        {type: JOB_DETAILS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export const REFLECTION_JOBS_REQUEST = 'REFLECTION_JOBS_REQUEST';
export const REFLECTION_JOBS_SUCCESS = 'REFLECTION_JOBS_SUCCESS';
export const REFLECTION_JOBS_FAILURE = 'REFLECTION_JOBS_FAILURE';

export function loadReflectionJobs(reflectionId, viewId) {
  const meta = { reflectionId, viewId };

  const apiCall = new APIV2Call()
    .path('jobs')
    .path('reflection')
    .path(reflectionId);

  return {
    [RSAA]:{
      types: [
        {type: REFLECTION_JOBS_REQUEST, meta},
        {type: REFLECTION_JOBS_SUCCESS, meta},
        {type: REFLECTION_JOBS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export const REFLECTION_JOB_DETAILS_REQUEST = 'REFLECTION_JOB_DETAILS_REQUEST';
export const REFLECTION_JOB_DETAILS_SUCCESS = 'REFLECTION_JOB_DETAILS_SUCCESS';
export const REFLECTION_JOB_DETAILS_FAILURE = 'REFLECTION_JOB_DETAILS_FAILURE';

export function loadReflectionJobDetails(jobId, reflectionId, viewId) {
  const meta = { jobId, viewId };

  const apiCall = new APIV2Call()
    .path('job')
    .path(jobId)
    .path('reflection')
    .path(reflectionId)
    .path('details');

  return {
    [RSAA]:{
      types: [
        {type: REFLECTION_JOB_DETAILS_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(REFLECTION_JOB_DETAILS_SUCCESS, jobDetailsSchema, meta),
        {type: REFLECTION_JOB_DETAILS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export const CANCEL_JOB_REQUEST = 'CANCEL_JOB_REQUEST';
export const CANCEL_JOB_SUCCESS = 'CANCEL_JOB_SUCCESS';
export const CANCEL_JOB_FAILURE = 'CANCEL_JOB_FAILURE';

const cancelJob = (jobId) => {
  const apiCall = new APIV2Call().paths(`job/${jobId}/cancel`);

  return {
    [RSAA]: {
      types: [
        CANCEL_JOB_REQUEST,
        CANCEL_JOB_SUCCESS,
        CANCEL_JOB_FAILURE
      ],
      method: 'POST',
      endpoint: apiCall
    }
  };
};

export const cancelJobAndShowNotification = (jobId) => (dispatch) => {
  return dispatch(cancelJob(jobId))
    .then(action => dispatch(addNotification(action.payload.message, 'success' )));
};

export const CANCEL_REFLECTION_JOB_REQUEST = 'CANCEL_REFLECTION_JOB_REQUEST';
export const CANCEL_REFLECTION_JOB_SUCCESS = 'CANCEL_REFLECTION_JOB_SUCCESS';
export const CANCEL_REFLECTION_JOB_FAILURE = 'CANCEL_REFLECTION_JOB_FAILURE';

const cancelReflectionJob = (jobId, reflectionId) => {
  const apiCall = new APIV2Call()
    .path('job')
    .path(jobId)
    .path('reflection')
    .path(reflectionId)
    .path('cancel');

  return {
    [RSAA]: {
      types: [
        CANCEL_REFLECTION_JOB_REQUEST,
        CANCEL_REFLECTION_JOB_SUCCESS,
        CANCEL_REFLECTION_JOB_FAILURE
      ],
      method: 'POST',
      endpoint: apiCall
    }
  };
};


export const cancelReflectionJobAndShowNotification = (jobId, reflectionId) => (dispatch) => {
  return dispatch(cancelReflectionJob(jobId, reflectionId))
    .then(action => dispatch(addNotification(action.payload.message, 'success' )));
};

export const ASK_GNARLY_STARTED = 'ASK_GNARLY_STARTED';
export const ASK_GNARLY_SUCCESS = 'ASK_GNARLY_SUCCESS';
export const ASK_GNARLY_FAILURE = 'ASK_GNARLY_FAILURE';

function fetchAskGnarly(jobId) {
  const apiCall = new APIV2Call().paths(`support/${jobId}`);

  return {
    [RSAA]:{
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
      endpoint: apiCall
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
    const tempApiCall = new APIV2Call()
      .path('temp-token')
      .params({
        'durationSeconds': 30,
        'request': '/apiv2' + profileUrl
      });
    fetch(tempApiCall.toString(), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': localStorageUtils.getAuthToken()
      }
    })
      .then(res => res.json())
      .then(data => {
        const apiCall = new APIV2Call()
          .fullpath(profileUrl)
          .param('.token', data.token ? data.token : '' );
        const location = getState().routing.locationBeforeTransitions;
        return dispatch(
          push({...location, state: {
            modal: 'JobProfileModal',
            profileUrl: apiCall.toString()
          }})
        );
      });
  };
}

export function showReflectionJobProfile(profileUrl, reflectionId) {
  return (dispatch, getState) => {
    // add the reflection id to the end of the url, but before the ? (url always has ?attemptId)
    const split = profileUrl.split('/');
    split[2] = split[2].replace('?', `/reflection/${reflectionId}?`);

    const apiCall = new APIV2Call()
      .fullpath(split.join('/'))
      .param('Authorization', localStorageUtils.getAuthToken());

    const location = getState().routing.locationBeforeTransitions;
    return dispatch(
      push({...location, state: {
        modal: 'JobProfileModal',
        profileUrl: apiCall.toString()
      }})
    );
  };
}

export const SET_CLUSTER_TYPE = 'SET_CLUSTER_TYPE';

export const setClusterType = value => ({
  type: SET_CLUSTER_TYPE,
  payload: value
});
