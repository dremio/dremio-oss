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
import { take, race, put, select, takeLatest } from 'redux-saga/effects';
import {
  TRANSFORM_PEEK_START,
  TRANSFORM_PEEK_SUCCESS,
  navigateToTransformPeek
} from 'actions/explore/dataset/peek';

import { RUN_TABLE_TRANSFORM_START } from 'actions/explore/dataset/common';

import { getLocation } from 'selectors/routing';

import { getLocationChangePredicate } from './utils';

export default function* transformPeek() {
  yield takeLatest(TRANSFORM_PEEK_START, handleAutoPeek); // yes, takeLatest: DX-4810
}

export function* handleAutoPeek({ meta }) {
  const location = yield select(getLocation);

  const { success } = yield race({
    success: take(action => action.type === TRANSFORM_PEEK_SUCCESS && action.meta.peekId === meta.peekId),
    anotherRequest: take([TRANSFORM_PEEK_START, RUN_TABLE_TRANSFORM_START]),
    locationChange: take(getLocationChangePredicate(location))
  });
  if (success && !success.error) {
    yield put(navigateToTransformPeek(success.payload.get('result')));
  }
}
