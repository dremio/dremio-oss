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
import { put, take, select, takeEvery } from 'redux-saga/effects';
import { replace } from 'react-router-redux';

import { ADD_NEW_USER_SUCCESS } from 'actions/admin';
import {
  LOGIN_USER_SUCCESS,
  fetchLoginUser
} from 'actions/account';

const getLocation = state => state.routing.locationBeforeTransitions;

export default function* signup() {
  yield takeEvery(ADD_NEW_USER_SUCCESS, handleSignup);
}

export function* handleSignup({ meta }) {
  const { form, isFirstUser } = meta;
  const { userName, password } = form;
  if (!isFirstUser) {
    return;
  }
  const location = yield select(getLocation);
  yield put(fetchLoginUser({userName, password}));
  yield take(LOGIN_USER_SUCCESS);
  yield put(replace({
    ...location,
    pathname: '/'
  }));
}
