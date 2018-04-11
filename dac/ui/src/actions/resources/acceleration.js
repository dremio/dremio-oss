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
import { arrayOf } from 'normalizr';

import { API_URL_V2 } from 'constants/Api';
import accelerationSchema from 'schemas/acceleration';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { constructFullPathAndEncode } from 'utils/pathUtils';
import { getDatasetAccelerationRequest } from 'dyn-load/actions/resources/accelerationRequest';
import {makeUncachebleURL} from 'ie11.js';

export const LOAD_ACCELERATION_START = 'LOAD_ACCELERATION_START';
export const LOAD_ACCELERATION_SUCCESS = 'LOAD_ACCELERATION_SUCCESS';
export const LOAD_ACCELERATION_FAILURE = 'LOAD_ACCELERATION_FAILURE';

export const CREATE_ACCELERATION_START = 'CREATE_ACCELERATION_START';
export const CREATE_ACCELERATION_SUCCESS = 'CREATE_ACCELERATION_SUCCESS';
export const CREATE_ACCELERATION_FAILURE = 'CREATE_ACCELERATION_FAILURE';

const fetchEmptyAcceleration = (dataset, viewId) => {
  const meta = { viewId, dataset };
  return {
    [CALL_API]: {
      types: [
        {type: CREATE_ACCELERATION_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(CREATE_ACCELERATION_SUCCESS, accelerationSchema, meta),
        {type: CREATE_ACCELERATION_FAILURE, meta}
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dataset.get('fullPathList').toJS()),
      endpoint: `${API_URL_V2}/accelerations`
    }
  };
};

export function createEmptyAcceleration(dataset, viewId) {
  return (dispatch) => {
    return dispatch(fetchEmptyAcceleration(dataset, viewId));
  };
}

export const UPDATE_ACCELERATION_START = 'UPDATE_ACCELERATION_START';
export const UPDATE_ACCELERATION_SUCCESS = 'UPDATE_ACCELERATION_SUCCESS';
export const UPDATE_ACCELERATION_FAILURE = 'UPDATE_ACCELERATION_FAILURE';

const fetchUpdateAcceleration = (form, accelerationId) => ({
  [CALL_API]: {
    types: [
      UPDATE_ACCELERATION_START,
      schemaUtils.getSuccessActionTypeWithSchema(UPDATE_ACCELERATION_SUCCESS, accelerationSchema),
      UPDATE_ACCELERATION_FAILURE],
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(form),
    endpoint: `${API_URL_V2}/accelerations/${accelerationId}`
  }
});

export function updateAcceleration(form, accelerationId) {
  return (dispatch) => {
    return dispatch(fetchUpdateAcceleration(form, accelerationId)).then((response) => {
      // TODO Remove this after DX-5726
      if (!response.error) {
        const pathList = response.payload
          .getIn(['entities', 'acceleration', accelerationId, 'context', 'dataset', 'pathList']);
        dispatch(getDatasetAccelerationRequest(constructFullPathAndEncode(pathList)));
      }

      return response;
    });
  };
}

export const LOAD_ACCELERATIONS_START = 'LOAD_ACCELERATIONS_START';
export const LOAD_ACCELERATIONS_SUCCESS = 'LOAD_ACCELERATIONS_SUCCESS';
export const LOAD_ACCELERATIONS_FAILURE = 'LOAD_ACCELERATIONS_FAILURE';

const fetchAccelerations = (/*config,*/ viewId) => {
  // TODO: implement pagination, we set the limit to 1 million for now
  //const { filter = '', order = '', sort = '', offset = '', limit = '' } = config;
  const meta = {viewId};
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_ACCELERATIONS_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_ACCELERATIONS_SUCCESS,
          {accelerationList: arrayOf(accelerationSchema)}, meta),
        {type: LOAD_ACCELERATIONS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: makeUncachebleURL(`${API_URL_V2}/accelerations?limit=1000000`)
    }
  };
};

export const loadAccelerations = (/*config, */viewId) => (dispatch) => dispatch(fetchAccelerations(/*config, */viewId));

export const LOAD_ACCELERATION_BY_ID_START = 'LOAD_ACCELERATION_BY_ID_START';
export const LOAD_ACCELERATION_BY_ID_SUCCESS = 'LOAD_ACCELERATION_BY_ID_SUCCESS';
export const LOAD_ACCELERATION_BY_ID_FAILURE = 'LOAD_ACCELERATION_BY_ID_FAILURE';

function fetchLoadAccelerationById(accelerationId, viewId) {
  const meta = { viewId };
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_ACCELERATION_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_ACCELERATION_SUCCESS, accelerationSchema, meta),
        {type: LOAD_ACCELERATION_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: makeUncachebleURL(`${API_URL_V2}/accelerations/${encodeURIComponent(accelerationId)}`)
    }
  };
}

export const loadAccelerationById = (accelerationId, viewId) => {
  return (dispatch) => {
    return dispatch(fetchLoadAccelerationById(accelerationId, viewId));
  };
};

export const DELETE_ACCELERATION_START = 'DELETE_ACCELERATION_START';
export const DELETE_ACCELERATION_SUCCESS = 'DELETE_ACCELERATION_SUCCESS';
export const DELETE_ACCELERATION_FAILURE = 'DELETE_ACCELERATION_FAILURE';

function fetchDeleteAcceleration(accelerationId, viewId) {
  const meta = { viewId, accelerationId };
  const entityRemovePaths = [['acceleration', accelerationId], ['datasetAcceleration', accelerationId]];
  return {
    [CALL_API]: {
      types: [
        { type: DELETE_ACCELERATION_START, meta },
        { type: DELETE_ACCELERATION_SUCCESS, meta: { ...meta, success: true, entityRemovePaths } },
        {
          type: DELETE_ACCELERATION_FAILURE,
          meta: {
            notification: {
              message: la('There was an error clearing the Reflections for this dataset.'),
              level: 'error'
            }
          }
        }
      ],
      method: 'DELETE',
      endpoint: makeUncachebleURL(`${API_URL_V2}/accelerations/${encodeURIComponent(accelerationId)}`)
    }
  };
}

export const deleteAcceleration = (accelerationId, viewId) => {
  return (dispatch) => {
    return dispatch(fetchDeleteAcceleration(accelerationId, viewId));
  };
};
