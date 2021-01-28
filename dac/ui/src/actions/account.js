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

import intercomUtils from 'utils/intercomUtils';
import { addNotification } from 'actions/notification';
import { APIV2Call } from '@app/core/APICall';

export const LOGIN_USER_START = 'LOGIN_USER_START';
export const LOGIN_USER_SUCCESS = 'LOGIN_USER_SUCCESS';
export const LOGIN_USER_FAILURE = 'LOGIN_USER_FAILURE';

/**
 * A user session details. Should corresponds to UserLoginSession.java class
 * see https://github.com/dremio/dremio/blob/master/oss/dac/backend/src/main/java/com/dremio/dac/model/usergroup/UserLoginSession.java#L24
 * @typedef {Object} UserSessionInfo
 * @property {string} token - a use authentication token
 * @property {string} userName
 */

export const LOGIN_VIEW_ID = 'LoginForm';

export const getLoginMeta = userName => ({
  userName,
  viewId: LOGIN_VIEW_ID
});

/**
 * @param {*} form
 * @param {string} form.userName
 * @param {string} form.password
 */
export function loginUser(form) {
  const { userName } = form;
  const meta = getLoginMeta(userName);

  const apiCall = new APIV2Call().path('login');

  return {
    [RSAA]: {
      types: [
        { type: LOGIN_USER_START, meta },
        // should have a payload of {@see UserSessionInfo} form
        { type: LOGIN_USER_SUCCESS, meta },
        getLogInFailedBaseDescriptor(userName)
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
      endpoint: apiCall
    }
  };
}

// must be consistent with loginUser's success action
// could not reuse this action creator in loginUser, as it contains payload field. If we payload is
// provided api middleware would not override it with server response payload
/**
 * @param {UserSessionInfo} userInfo
 */
export const userLoggedIn = userInfo => ({
  type: LOGIN_USER_SUCCESS,
  payload: userInfo,
  meta: getLoginMeta(userInfo.userName)
});

export const getLogInFailedBaseDescriptor = userName => ({
  type: LOGIN_USER_FAILURE,
  meta: {
    ...getLoginMeta(userName),
    errorMessage: la('Authentication failed.')
  }
});

//must be consistent with loginUser's failure action
/**
 * @param {UserSessionInfo} userInfo
 */
export const userLogInFailed = (userName, errorMsg = null) => {
  const base = getLogInFailedBaseDescriptor(userName);
  base.meta.errorMessage = errorMsg || base.meta.errorMessage;
  base.error = true; // need to force view reducer treat the action as failure

  return base;
};

export const LOGOUT_USER_START = 'LOGOUT_USER_START';
export const LOGOUT_USER_SUCCESS = 'LOGOUT_USER_SUCCESS';
export const LOGOUT_USER_FAILURE = 'LOGOUT_USER_FAILURE';

export function logoutUser() {
  const apiCall = new APIV2Call().path('login');

  return {
    [RSAA]: {
      types: [
        { type: LOGOUT_USER_START },
        { type: LOGOUT_USER_SUCCESS },
        { type: LOGOUT_USER_FAILURE }
      ],
      method: 'DELETE',
      endpoint: apiCall
    }
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

  const apiCall = new APIV2Call()
    .path('user')
    .path(oldName ? oldName : accountData.userName)
    .params({version: accountData.version});

  return {
    [RSAA]: {
      types: [
        EDIT_ACCOUNT_START,
        { type: EDIT_ACCOUNT_SUCCESS, meta },
        EDIT_ACCOUNT_FAILURE
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(accountData),
      endpoint: apiCall
    }
  };
};

const intercomMissingHandler = dispatch => error => {
  console.warn(error);
  return dispatch(addNotification(la('Sorry, chat is not currently available.'), 'warning'));
};

// always fulfills ("success")
export const callIfChatAllowedOrWarn = (action = () => {}) => (dispatch) => {
  if (intercomUtils.ifChatAllowed(intercomMissingHandler(dispatch))) {
    action();
  }
};

export const CHECK_USER_START = 'CHECK_USER_START';
export const CHECK_USER_SUCCESS = 'CHECK_USER_SUCCESS';
export const CHECK_USER_FAILURE = 'CHECK_USER_FAILURE';

/**
 * Checks if any user exists in Dremio and wether or not current user token is valid
 *
 * Server returns:
 * a) 'No User Available' error in case if there is no any user in Dremio
 *    see https://github.com/dremio/dremio/blob/64ada2028fb042e6b3a035b9ef64814218095e30/oss/dac/backend/src/main/java/com/dremio/dac/server/NoUserFilter.java#L63
 *    This error would be handled by authMiddleware
 * b) Status = 401 if user is not authorized
 */
export const checkUser = () => {
  const apiCall = new APIV2Call()
    .path('login')
    .uncachable();

  return {
    [RSAA]: {
      types: [
        CHECK_USER_START,
        CHECK_USER_SUCCESS,
        CHECK_USER_FAILURE
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
};
