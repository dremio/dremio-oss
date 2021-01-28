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
import { call, put, select } from 'redux-saga/effects';
import { replace } from 'react-router-redux';

import { log } from '@app/utils/logger';
import { getLocation } from '@app/selectors/routing';
import socket from '@app/utils/socket';
import { LOGIN_PATH, SIGNUP_PATH, SSO_LANDING_PATH } from '../loginLogout';

export default function* handleAppInit() {
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
  if (pathname === LOGIN_PATH || pathname === SIGNUP_PATH || pathname === SSO_LANDING_PATH) { // redirect from login and sign up paths
    const redirectUrl = location.query.redirect || '/';
    log('redirect after login is started. Redirect url:', redirectUrl);
    yield put(replace(redirectUrl));
  }
}
