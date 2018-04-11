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

import { makeUncachebleURL } from 'ie11.js';

import spaceSchema from 'dyn-load/schemas/space';

import { API_URL_V2 } from 'constants/Api';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import actionUtils from 'utils/actionUtils/actionUtils';

export const SPACES_LIST_LOAD_START = 'SPACES_LIST_LOAD_START';
export const SPACES_LIST_LOAD_SUCCESS = 'SPACES_LIST_LOAD_SUCCESS';
export const SPACES_LIST_LOAD_FAILURE = 'SPACES_LIST_LOAD_FAILURE';

function fetchSpaceListData() {
  const meta = {viewId: 'AllSpaces', mergeEntities: true};
  return {
    [CALL_API]: {
      types: [
        { type: SPACES_LIST_LOAD_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(SPACES_LIST_LOAD_SUCCESS, { spaces: arrayOf(spaceSchema) }, meta),
        { type: SPACES_LIST_LOAD_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: API_URL_V2 + makeUncachebleURL('/spaces')
    }
  };
}

export function loadSpaceListData() {
  return (dispatch) => {
    return dispatch(fetchSpaceListData());
  };
}

export const SET_SPACE_PIN_STATE = 'SET_SPACE_PIN_STATE';

export function setSpacePin(spaceName, isActivePin) {
  return (dispatch) => {
    dispatch({
      type: SET_SPACE_PIN_STATE,
      name: spaceName,
      isActivePin
    });
  };
}

export const ADD_NEW_SPACE_START = 'ADD_NEW_SPACE_START';
export const ADD_NEW_SPACE_SUCCESS = 'ADD_NEW_SPACE_SUCCESS';
export const ADD_NEW_SPACE_FAILURE = 'ADD_NEW_SPACE_FAILURE';

function putSpace(space, isCreate) {

  const meta = {
    invalidateViewIds: ['AllSpaces'],
    mergeEntities: true,
    notification: {
      message: isCreate ? la('Successfully created.') : la('Successfully updated.'),
      level: 'success'
    }
  };
  return {
    [CALL_API]: {
      types: [
        ADD_NEW_SPACE_START,
        schemaUtils.getSuccessActionTypeWithSchema(ADD_NEW_SPACE_SUCCESS, spaceSchema, meta),
        ADD_NEW_SPACE_FAILURE
      ],
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(space),
      endpoint: `${API_URL_V2}/space/${encodeURIComponent(space.name)}`
    }
  };
}

export function createNewSpace(values) {
  return (dispatch) => {
    return dispatch(putSpace(values, true));
  };
}

export function updateSpace(values) {
  return (dispatch) => {
    return dispatch(putSpace(values, false));
  };
}

export const REMOVE_SPACE_START = 'REMOVE_SPACE_START';
export const REMOVE_SPACE_SUCCESS = 'REMOVE_SPACE_SUCCESS';
export const REMOVE_SPACE_FAILURE = 'REMOVE_SPACE_FAILURE';

function fetchRemoveSpace(space) {
  const meta = { name, id: space.get('id'), invalidateViewIds: ['AllSpaces'] };
  const errorMessage = la('There was an error removing the space.');
  const entityRemovePaths = [['space', space.get('id')]];

  return {
    [CALL_API]: {
      types: [
        {
          type: REMOVE_SPACE_START, meta
        },
        {
          type: REMOVE_SPACE_SUCCESS, meta: {...meta, success: true, entityRemovePaths, emptyEntityCache: space.get('name')}
        },
        {
          type: REMOVE_SPACE_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}${space.getIn(['links', 'self'])}?version=${space.get('version')}`
    }
  };
}

export function removeSpace(space) {
  return (dispatch) => {
    return dispatch(fetchRemoveSpace(space));
  };
}

export const RENAME_SPACE_START = 'RENAME_SPACE_START';
export const RENAME_SPACE_SUCCESS = 'RENAME_SPACE_SUCCESS';
export const RENAME_SPACE_FAILURE = 'RENAME_SPACE_FAILURE';

function fetchRenameSpace(oldName, newName) {
  return {
    [CALL_API]: {
      types: [
        {
          type: RENAME_SPACE_START,
          meta: { oldName, newName }
        },
        {
          type: RENAME_SPACE_SUCCESS,
          meta: { oldName }
        },
        {
          type: RENAME_SPACE_FAILURE,
          meta: { oldName }
        }
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}/space/${oldName}/rename?renameTo=${newName}`
    }
  };
}

export function renameSpace(oldName, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameSpace(oldName, newName)).then(() => dispatch(loadSpaceListData()));
  };
}
