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
import { API_URL_V2 } from 'constants/Api';

// todo: s/dataSettings/datasetSettings/ in project

// ACCELERATION

export const ACCELERATION_DATA_START = 'ACCELERATION_DATA_START';
export const ACCELERATION_DATA_SUCCESS = 'ACCELERATION_DATA_SUCCESS';
export const ACCELERATION_DATA_FAILURE = 'ACCELERATION_DATA_FAILURE';
export const ACCELERATION_UPDATE_START = 'ACCELERATION_UPDATE_START';
export const ACCELERATION_UPDATE_SUCCESS = 'ACCELERATION_UPDATE_SUCCESS';
export const ACCELERATION_UPDATE_FAILURE = 'ACCELERATION_UPDATE_FAILURE';

function fetchAccelerationData(cpath) {
  return {
    [CALL_API]: {
      types: [ACCELERATION_DATA_START, ACCELERATION_DATA_SUCCESS, ACCELERATION_DATA_FAILURE], // todo: should get a schema up in here
      method: 'GET',
      endpoint: `${API_URL_V2}/acceleration/${cpath}`
    }
  };
}

export function loadAccelerationData(cpath) {
  return (dispatch) => {
    return dispatch(fetchAccelerationData(cpath));
  };
}

function putAccelerationSchedule(cpath, config) {
  return {
    [CALL_API]: {
      types: [ACCELERATION_UPDATE_START, ACCELERATION_UPDATE_SUCCESS, ACCELERATION_UPDATE_FAILURE],
      method: 'PUT',
      body: JSON.stringify(config),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}/acceleration/${cpath}`
    }
  };
}

export function updateAccelerationSchedule(cpath, config) {
  return (dispatch) => {
    return dispatch(putAccelerationSchedule(cpath, config));
  };
}
