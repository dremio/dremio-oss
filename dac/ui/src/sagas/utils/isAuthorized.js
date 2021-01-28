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
import { put } from 'redux-saga/effects';

import { log } from '@app/utils/logger';
import {
  checkUser,
  CHECK_USER_SUCCESS
} from '@app/actions/account';

export function* isAuthorized() {
  log('send request to the server to check is a user authorized');
  const promise = yield put(checkUser());

  // wait for response
  const action = yield promise;
  log('response action', action);
  return action.type === CHECK_USER_SUCCESS;
}
