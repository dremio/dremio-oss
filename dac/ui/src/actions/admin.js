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

import schemaUtils from 'utils/apiUtils/schemaUtils';

import { arrayOf } from 'normalizr';
import userSchema from 'schemas/user';

import { APIV2Call } from '@app/core/APICall';

export const SOURCE_NODES_START = 'SOURCE_NODES_START';
export const SOURCE_NODES_SUCCESS = 'SOURCE_NODES_SUCCESS';
export const SOURCE_NODES_FAILURE = 'SOURCE_NODES_FAILURE';

// NODES

function fetchNodeCredentials(viewId) {
  const meta = {viewId};

  const apiCall = new APIV2Call().paths('system/nodes');

  return {
    [RSAA]: {
      types: [
        {type: SOURCE_NODES_START, meta},
        {type: SOURCE_NODES_SUCCESS, meta},
        {type: SOURCE_NODES_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadNodeCredentials(viewId) {
  return (dispatch) => {
    return dispatch(fetchNodeCredentials(viewId));
  };
}

// USERS. todo: move this to resources and DRY up
// todo: users calls should utilize the new user id, not the userName

export const USERS_VIEW_ID = 'USERS_VIEW_ID';

export const LOAD_FILTERED_USER_START = 'LOAD_FILTERED_USER_START';
export const LOAD_FILTERED_USER_SUCCESS = 'LOAD_FILTERED_USER_SUCCESS';
export const LOAD_FILTERED_USER_FAILURE = 'LOAD_FILTERED_USER_FAILURE';

// todo: backend doesn't actually do filtering yet
function fetchFilteredUsers(value = '') {
  const meta = {viewId: USERS_VIEW_ID}; // todo: see need ability to list users from anywhere

  const apiCall = new APIV2Call();

  if (value) {
    apiCall
      .paths('users/search')
      .params({filter: value});
  } else {
    apiCall
      .paths('users/all')
      .uncachable();
  }

  return {
    [RSAA]: {
      types: [
        {type: LOAD_FILTERED_USER_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_FILTERED_USER_SUCCESS, { users: arrayOf(userSchema) }, meta),
        {type: LOAD_FILTERED_USER_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function searchUsers(value) {
  return (dispatch) => {
    return dispatch(fetchFilteredUsers(value));
  };
}


export const CREATE_FIRST_USER_START = 'CREATE_FIRST_USER_START';
export const CREATE_FIRST_USER_SUCCESS = 'CREATE_FIRST_USER_SUCCESS';
export const CREATE_FIRST_USER_FAILURE = 'CREATE_FIRST_USER_FAILURE';

export function createFirstUser(form, meta) {
  const { viewId } = meta || {};
  const metaSuccess = {
    invalidateViewIds: [USERS_VIEW_ID],
    notification: {
      message: la('Successfully created.'),
      level: 'success'
    },
    form
  };

  const apiCall = new APIV2Call()
    .path('bootstrap')
    .path('firstuser');

  return {
    [RSAA]: {
      types: [
        { type: CREATE_FIRST_USER_START },
        { type: CREATE_FIRST_USER_SUCCESS, meta: metaSuccess },
        { type: CREATE_FIRST_USER_FAILURE, meta: {viewId} }
      ],
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(form),
      endpoint: apiCall
    }
  };
}

export const REMOVE_USER_START = 'REMOVE_USER_START';
export const REMOVE_USER_SUCCESS = 'REMOVE_USER_SUCCESS';
export const REMOVE_USER_FAILURE = 'REMOVE_USER_FAILURE';

function deleteUser(user) {
  const meta = {
    invalidateViewIds: [USERS_VIEW_ID],
    notification: {
      message: la('Successfully removed.'),
      level: 'success'
    }
  };

  const apiCall = new APIV2Call()
    .path('user')
    .path(user.get('userName'))
    .params({version: user.getIn(['userConfig', 'version'])});

  return {
    [RSAA]: {
      types: [
        REMOVE_USER_START,
        { type: REMOVE_USER_SUCCESS, meta },
        { type: REMOVE_USER_FAILURE, meta: { notification: true } }
      ],
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      endpoint: apiCall
    }
  };
}

export function removeUser(user) {
  return (dispatch) => {
    return dispatch(deleteUser(user));
  };
}
