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
import { VIEW_ID as HOME_CONTENTS_VIEW_ID } from 'pages/HomePage/subpages/HomeContents';

import { constructFullPathAndEncode } from 'utils/pathUtils';

import sqlFunctions from 'customData/sqlFunctions.json';

export const CREATE_DATASET_START = 'CREATE_DATASET_START';
export const CREATE_DATASET_SUCCESS = 'CREATE_DATASET_SUCCESS';
export const CREATE_DATASET_FAILURE = 'CREATE_DATASET_FAILURE';

function putDataset(cpath, dataset) {
  return {
    [CALL_API]: {
      types: [CREATE_DATASET_START, CREATE_DATASET_SUCCESS, CREATE_DATASET_FAILURE],
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dataset),
      endpoint: `${API_URL_V2}/dataset${cpath}`
    }
  };
}

export function createDataset(nodeId, dataset, version, asPath) {
  return (dispatch) => {
    return dispatch(putDataset(nodeId, dataset, version, asPath));
  };
}

export const CREATE_DATASET_FROM_EXISTING_START = 'CREATE_DATASET_FROM_EXISTING_START';
export const CREATE_DATASET_FROM_EXISTING_SUCCESS = 'CREATE_DATASET_FROM_EXISTING_SUCCESS';
export const CREATE_DATASET_FROM_EXISTING_FAILURE = 'CREATE_DATASET_FAILURE';

function putDatasetFromExisting(fullPathFrom, fullPathTo, datasetConfig) {
  const meta = { invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };
  const encodedPathFrom = constructFullPathAndEncode(fullPathFrom);
  const encodedPathTo = constructFullPathAndEncode(fullPathTo);

  return {
    [CALL_API]: {
      types: [
        {type: CREATE_DATASET_FROM_EXISTING_START, meta},
        {type: CREATE_DATASET_FROM_EXISTING_SUCCESS, meta},
        {type: CREATE_DATASET_FROM_EXISTING_FAILURE, meta}
      ],
      method: 'PUT',
      body: JSON.stringify(datasetConfig),
      endpoint: `${API_URL_V2}/dataset/${encodedPathTo}/copyFrom/${encodedPathFrom}`
    }
  };
}

export function createDatasetFromExisting() {
  return (dispatch) => {
    return dispatch(putDatasetFromExisting(...arguments));
  };
}

export const MOVE_DATASET_START = 'MOVE_DATASET_START';
export const MOVE_DATASET_SUCCESS = 'MOVE_DATASET_SUCCESS';
export const MOVE_DATASET_FAILURE = 'MOVE_DATASET_FAILURE';

function fetchDataSetMove(fullPathFrom, fullPathTo) {
  const meta = { invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };
  const encodedPathFrom = constructFullPathAndEncode(fullPathFrom);
  const encodedPathTo = constructFullPathAndEncode(fullPathTo);
  return {
    [CALL_API]: {
      types: [
        { type: MOVE_DATASET_START, meta },
        { type: MOVE_DATASET_SUCCESS, meta },
        { type: MOVE_DATASET_FAILURE, meta }
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}/dataset/${encodedPathFrom}/moveTo/${encodedPathTo}`
    }
  };
}

export function moveDataSet(cPathFrom, cPathTo) {
  return (dispatch) => {
    return dispatch(fetchDataSetMove(cPathFrom, cPathTo));
  };
}

export const SQL_HELP_FUNC_SUCCESS = 'SQL_HELP_FUNC_SUCCESS';

export function loadHelpGridData(pattern) {
  const sqlFuncs = pattern
    ? sqlFunctions.filter(func => {
      return func.name.toLowerCase().indexOf(pattern.toLowerCase()) !== -1 ||
             func.tags && func.tags.find(tag => tag.toLowerCase().indexOf(pattern.toLowerCase()) !== -1);
    })
    : sqlFunctions;

  return {
    type: SQL_HELP_FUNC_SUCCESS,
    meta: { sqlFuncs }
  };
}
