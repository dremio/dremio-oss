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

import { API_URL, API_URL_V2} from 'constants/Api';

import intercomUtils from 'utils/intercomUtils';
import { addNotification } from 'actions/notification';

export const SOURCE_CREDENTIAL_START = 'SOURCE_CREDENTIAL_START';
export const SOURCE_CREDENTIAL_SUCCESS = 'SOURCE_CREDENTIAL_SUCCESS';
export const SOURCE_CREDENTIAL_FAILURE = 'SOURCE_CREDENTIAL_FAILURE';

function fetchSourceCredentials() {
  return {
    [CALL_API]: {
      types: [SOURCE_CREDENTIAL_START, SOURCE_CREDENTIAL_SUCCESS, SOURCE_CREDENTIAL_FAILURE],
      method: 'GET',
      endpoint: API_URL + '/user/datastore'
    }
  };
}

export function loadSourceCredentials() {
  return (dispatch) => {
    return dispatch(fetchSourceCredentials());
  };
}

export const ADD_SOURCE_CREDENTIAL = 'ADD_SOURCE_CREDENTIAL';

export function addSourceCredential() {
  return (dispatch) => {
    const action = { type: ADD_SOURCE_CREDENTIAL, credential: 'some new credential' };
    dispatch(action);
  };
}

export const REMOVE_SOURCE_CREDENTIAL = 'REMOVE_SOURCE_CREDENTIAL';

export function removeSourceCredential(index) {
  return (dispatch) => {
    const action = { type: REMOVE_SOURCE_CREDENTIAL, index };
    dispatch(action);
  };
}

export const GET_API_KEY_START = 'GET_API_KEY_START';
export const GET_API_KEY_SUCCESS = 'GET_API_KEY_SUCCESS';
export const GET_API_KEY_FAILURE = 'GET_API_KEY_FAILURE';

function fetchApiKey() {
  return {
    [CALL_API]: {
      types: [GET_API_KEY_START, GET_API_KEY_SUCCESS, GET_API_KEY_FAILURE],
      method: 'GET',
      endpoint: API_URL + '/user/apikey'
    }
  };
}

export function generateApiKey() {
  return (dispatch) => {
    return dispatch(fetchApiKey());
  };
}

export const CONNECT_BI_TOOL_START = 'CONNECT_BI_TOOL_START';
export const CONNECT_BI_TOOL_SUCCESS = 'CONNECT_BI_TOOL_SUCCESS';
export const CONNECT_BI_TOOL_FAILURE = 'CONNECT_BI_TOOL_FAILURE';

function fetchSetConnectionBiTool(value) {
  return {
    [CALL_API]: {
      types: [CONNECT_BI_TOOL_START, CONNECT_BI_TOOL_SUCCESS, CONNECT_BI_TOOL_FAILURE],
      method: 'GET',
      endpoint: `${API_URL}/user/BiTool/${value}`
    }
  };
}

export function setConnectionBiTool(value) {
  return (dispatch) => {
    return dispatch(fetchSetConnectionBiTool(value));
  };
}

export const LOGIN_USER_START = 'LOGIN_USER_START';
export const LOGIN_USER_SUCCESS = 'LOGIN_USER_SUCCESS';
export const LOGIN_USER_FAILURE = 'LOGIN_USER_FAILURE';

export function fetchLoginUser(form, viewId) {
  const meta = {
    form,
    viewId
  };
  return {
    [CALL_API]: {
      types: [
        { type: LOGIN_USER_START, meta },
        { type: LOGIN_USER_SUCCESS, meta },
        { type: LOGIN_USER_FAILURE, meta: {...meta, errorMessage: la('Authentication failed.')}}
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
      endpoint: `${API_URL_V2}/login`
    }
  };
}

export function loginUser(value, viewId) {
  return (dispatch) => {
    return dispatch(fetchLoginUser(value, viewId));
  };
}

export const LOGOUT_USER_START = 'LOGOUT_USER_START';
export const LOGOUT_USER_SUCCESS = 'LOGOUT_USER_SUCCESS';
export const LOGOUT_USER_FAILURE = 'LOGOUT_USER_FAILURE';

export function fetchLogoutUser() {
  return {
    [CALL_API]: {
      types: [
        { type: LOGOUT_USER_START },
        { type: LOGOUT_USER_SUCCESS },
        { type: LOGOUT_USER_FAILURE }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}/login`
    }
  };
}

export function logoutUser() {
  return (dispatch) => {
    return dispatch(fetchLogoutUser());
  };
}

export const UNAUTHORIZED_ERROR = 'UNAUTHORIZED_ERROR';

export function unauthorizedError() {
  return {
    type: UNAUTHORIZED_ERROR,
    error: true
  };
}

export const NO_USERS_ERROR = 'NO_USERS_ERROR';

export function noUsersError() {
  return {
    type: NO_USERS_ERROR,
    error: true
  };
}

export const editAccount = (accountData, oldName) => dispatch => dispatch(fetchEditAccount(accountData, oldName));

export const EDIT_ACCOUNT_START = 'EDIT_ACCOUNT_START';
export const EDIT_ACCOUNT_SUCCESS = 'EDIT_ACCOUNT_SUCCESS';
export const EDIT_ACCOUNT_FAILURE = 'EDIT_ACCOUNT_FAILURE';

const fetchEditAccount = (accountData, oldName) => {
  const meta = { notification: { message: 'Changes were successfully saved', level: 'success' } };
  return {
    [CALL_API]: {
      types: [
        EDIT_ACCOUNT_START,
        { type: EDIT_ACCOUNT_SUCCESS, meta },
        EDIT_ACCOUNT_FAILURE
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(accountData),
      endpoint: `${API_URL_V2}/user/${oldName ? oldName : accountData.userName}?version=${accountData.version}`
    }
  };
};

// always fulfills ("success")
export const callIfChatAllowedOrWarn = (action = () => {}) => (dispatch) => {
  return intercomUtils.ifChatAllowed().then(action, (error) => {
    console.warn(error);
    return dispatch(addNotification(la('Sorry, chat is not currently available.'), 'warning'));
  });
};

export const CHECK_FOR_FIRST_USER_START = 'CHECK_FOR_FIRST_USER_START';
export const CHECK_FOR_FIRST_USER_SUCCESS = 'CHECK_FOR_FIRST_USER_SUCCESS';
export const CHECK_FOR_FIRST_USER_FAILURE = 'CHECK_FOR_FIRST_USER_FAILURE';

const fetchCheckForFirstUser = () => {
  return {
    [CALL_API]: {
      types: [
        CHECK_FOR_FIRST_USER_START,
        CHECK_FOR_FIRST_USER_SUCCESS,
        CHECK_FOR_FIRST_USER_FAILURE
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}/login`
    }
  };
};

export function checkForFirstUser() {
  return (dispatch) => {
    return dispatch(fetchCheckForFirstUser());
  };
}
