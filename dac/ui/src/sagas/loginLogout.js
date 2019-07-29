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
import { call, put, select, takeLatest } from 'redux-saga/effects';
import { push, replace } from 'react-router-redux';

import { log } from '@app/utils/logger';
import { getLocation } from '@app/selectors/routing';
import {
  LOGIN_USER_SUCCESS, LOGOUT_USER_SUCCESS, checkUser,
  CHECK_USER_SUCCESS, NO_USERS_ERROR, UNAUTHORIZED_ERROR
} from '@app/actions/account';
import intercomUtils from '@app/utils/intercomUtils';
import socket from '@app/utils/socket';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';

//#region Route constants. Unfortunately should store these constants here (not in routes.js) to
// avoid module circular references

export const SIGNUP_PATH = '/signup';
export const LOGIN_PATH = '/login';

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
//export for testing
/**
 * This method assumes that user is authorized and session is not expired
 */
export function* handleAppInit() {
  if (isAppInit) {
    log('App is already initialiazed. Nothing to do.');
    return;
  }
  log('intercom util start');
  // start intercom and open socket for cases where a user is already logged in
  yield call([intercomUtils, intercomUtils.boot]);

  if (socket.exists) {
    // by some reason socket exists. we should close it as it could belong to other user
    log('Close a socket before re-opening');
    yield call([socket, socket.close]);
  }
  log('open socket');
  yield call([socket, socket.open]);

  const location = yield select(getLocation);
  log('current location', location);
  const { pathname } = location;
  if (pathname === LOGIN_PATH || pathname === SIGNUP_PATH) { // redirect from login and sign up paths
    const redirectUrl = location.query.redirect || '/';
    log('redirect after login is started. Redirect url:', redirectUrl);
    yield put(replace(redirectUrl));
  }
  isAppInit = true;
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

// export for testing only
export function* isAuthorized() {
  log('send request to the server to check is a user authorized');
  const promise = yield put(checkUser());

  // wait for response
  const action = yield promise;
  log('response action', action);
  return action.type === CHECK_USER_SUCCESS;
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
