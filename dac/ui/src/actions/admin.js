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

import schemaUtils from 'utils/apiUtils/schemaUtils';
import { API_URL_V2 } from 'constants/Api';

import { arrayOf } from 'normalizr';
import userSchema from 'schemas/user';


import {makeUncachebleURL} from 'ie11.js';

export const SOURCE_NODES_START = 'SOURCE_NODES_START';
export const SOURCE_NODES_SUCCESS = 'SOURCE_NODES_SUCCESS';
export const SOURCE_NODES_FAILURE = 'SOURCE_NODES_FAILURE';

// NODES

function fetchNodeCredentials(viewId) {
  const meta = {viewId};
  return {
    [CALL_API]: {
      types: [
        {type: SOURCE_NODES_START, meta},
        {type: SOURCE_NODES_SUCCESS, meta},
        {type: SOURCE_NODES_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/system/nodes`
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
  const encodedValue = encodeURIComponent(value);
  const meta = {viewId: USERS_VIEW_ID}; // todo: see need ability to list users from anywhere
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_FILTERED_USER_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_FILTERED_USER_SUCCESS, { users: arrayOf(userSchema) }, meta),
        {type: LOAD_FILTERED_USER_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/users/${!encodedValue ? makeUncachebleURL('all') : `search?filter=${encodedValue}`}` // todo: why isn't the search uncacheable if 'all' is?
    }
  };
}

export function searchUsers(value) {
  return (dispatch) => {
    return dispatch(fetchFilteredUsers(value));
  };
}


export const ADD_NEW_USER_START = 'ADD_NEW_USER_START';
export const ADD_NEW_USER_SUCCESS = 'ADD_NEW_USER_SUCCESS';
export const ADD_NEW_USER_FAILURE = 'ADD_NEW_USER_FAILURE';

function putUser(form, meta) {
  const { isFirstUser, viewId } = meta || {};
  const metaSuccess = {
    invalidateViewIds: [USERS_VIEW_ID],
    notification: {
      message: la('Successfully created.'),
      level: 'success'
    },
    isFirstUser,
    form
  };
  const endpoint = !isFirstUser
    ? `${API_URL_V2}/user/${encodeURIComponent(form.userName)}`
    : `${API_URL_V2}/bootstrap/firstuser`;
  return {
    [CALL_API]: {
      types: [
        { type: ADD_NEW_USER_START, meta: {isFirstUser, form} },
        { type: ADD_NEW_USER_SUCCESS, meta: metaSuccess },
        { type: ADD_NEW_USER_FAILURE, meta: {viewId} }
      ],
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(form),
      endpoint
    }
  };
}

export function createNewUser(values) {
  return (dispatch) => {
    return dispatch(putUser(values));
  };
}

export function createFirstUser(values, meta) {
  return (dispatch) => {
    return dispatch(putUser(values, {...meta, isFirstUser: true }));
  };
}


export const EDIT_USER_START = 'EDIT_USER_START';
export const EDIT_USER_SUCCESS = 'EDIT_USER_SUCCESS';
export const EDIT_USER_FAILURE = 'EDIT_USER_FAILURE';

function postEditUser(values, oldName) {
  const meta = {
    invalidateViewIds: [USERS_VIEW_ID],
    notification: {
      message: la('Successfully updated.'),
      level: 'success'
    }
  };
  return {
    [CALL_API]: {
      types: [
        EDIT_USER_START,
        { type: EDIT_USER_SUCCESS, meta },
        EDIT_USER_FAILURE
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(values),
      endpoint: `${API_URL_V2}/user/${oldName}`
    }
  };
}

export function editUser(values, oldName) {
  return (dispatch) => {
    return dispatch(postEditUser(values, oldName));
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
  return {
    [CALL_API]: {
      types: [
        REMOVE_USER_START,
        { type: REMOVE_USER_SUCCESS, meta },
        { type: REMOVE_USER_FAILURE, meta: { notification: true } }
      ],
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${user.getIn(['links', 'self'])}?version=${user.getIn(['userConfig', 'version'])}`
    }
  };
}

export function removeUser(user) {
  return (dispatch) => {
    return dispatch(deleteUser(user));
  };
}
