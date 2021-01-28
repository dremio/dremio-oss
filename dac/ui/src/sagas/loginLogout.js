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
import { all, call, fork, put, takeLatest } from 'redux-saga/effects';
import { push } from 'react-router-redux';

import { log } from '@app/utils/logger';
import {
  LOGIN_USER_SUCCESS, LOGOUT_USER_SUCCESS, NO_USERS_ERROR, UNAUTHORIZED_ERROR
} from '@app/actions/account';
import intercomUtils from '@app/utils/intercomUtils';
import socket from '@app/utils/socket';
import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';
import { isAuthorized } from '@inject/sagas/utils/isAuthorized';
import { default as handleAppInitHelper } from '@inject/sagas/utils/handleAppInit';

//#region Route constants. Unfortunately should store these constants here (not in routes.js) to
// avoid module circular references

export const SIGNUP_PATH = '/signup';
export const LOGIN_PATH = '/login';
export const SSO_LANDING_PATH = '/login/sso/landing';

export function getLoginUrl() {
  return `${LOGIN_PATH}?redirect=${encodeURIComponent(window.location.href.slice(window.location.origin.length))}`;
}

//#endregion

export function* afterLogin() {
  yield takeLatest(LOGIN_USER_SUCCESS, handleLogin);
}

export function* afterLogout() {
  yield takeLatest(LOGOUT_USER_SUCCESS, handleLogout);
}

export function* afterAppStop() {
  yield takeLatest([NO_USERS_ERROR, UNAUTHORIZED_ERROR], handleAppStop);
}

//export for testing
export function* handleLogin({ payload }) {
  log('Add user data to local storage', payload);
  yield call([localStorageUtils, localStorageUtils.setUserData], payload);

  yield call(handleAppInit);
}

// export for testing only
export function* checkAppState() {
  // We should always make this call to cover first user flow.
  const isUserValid = yield call(isAuthorized);
  log('Is user valid', isUserValid);

  if (!isUserValid) {
    log('clear user data and token as a user is invalid');
    yield call([localStorageUtils, localStorageUtils.clearUserData]);
    return;
  }
  yield call(handleAppInit);
}

let isAppInit = false;
//export for testing
export const resetAppInitState = () => {
  isAppInit = false;
};

export function* handleAppInit() {
  if (isAppInit) {
    log('App is already initialiazed. Nothing to do.');
    return;
  }
  yield call(handleAppInitHelper);
  isAppInit = true; // eslint-disable-line require-atomic-updates
}

//export for testing
export function* handleAppStop() {
  log('intercom cleanup');
  yield call([intercomUtils, intercomUtils.shutdown]);
  if (socket.exists) {
    log('socket close');
    yield call([socket, socket.close]);
  }
  isAppInit = false;
}

function* handleLogout() {
  /*
    must be before localStorageUtils.clearUserData, as we use user data to check if a user is authorized
    to use intercom
  */
  yield call(handleAppStop);
  log('clear user data and token');
  yield call([localStorageUtils, localStorageUtils.clearUserData]);
  log('go to login page');
  yield put(push(getLoginUrl()));
}

export default function* loginLogoutSagas() {
  yield all([
    fork(afterLogin),
    fork(afterLogout),
    fork(afterAppStop)
  ]);
}
