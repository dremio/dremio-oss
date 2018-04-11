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
import { take, takeEvery, race, put } from 'redux-saga/effects';

import {
  TRANSFORM_CARD_PREVIEW_START,
  TRANSFORM_CARD_PREVIEW_SUCCESS,
  TRANSFORM_CARD_PREVIEW_FAILURE,
  RESET_RECOMMENDED_TRANSFORMS,
  updateTransformCard
} from 'actions/explore/recommended';

export function getRequestPredicate(actionType, meta) {
  return (action) =>
    action.type === actionType &&
    action.meta.transformType === meta.transformType &&
    action.meta.method === meta.method &&
    action.meta.index === meta.index;
}

export default function* transformCardPreview() {
  yield takeEvery(TRANSFORM_CARD_PREVIEW_START, handleTransformCardPreview);
}

export function* handleTransformCardPreview({ meta }) {
  const { success } = yield race({
    success: take(getRequestPredicate(TRANSFORM_CARD_PREVIEW_SUCCESS, meta)),
    failure: take(getRequestPredicate(TRANSFORM_CARD_PREVIEW_FAILURE, meta)),
    anotherRequest: take(getRequestPredicate(TRANSFORM_CARD_PREVIEW_START, meta)),
    reset: take(RESET_RECOMMENDED_TRANSFORMS)
  });
  if (success && !success.error) {
    yield put(updateTransformCard(success.payload, success.meta));
  }
}


