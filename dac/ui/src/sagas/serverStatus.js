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
import { take, race, call, put } from 'redux-saga/effects';
import { delay } from 'redux-saga';

import {
  SCHEDULE_CHECK_SERVER_STATUS,
  UNSCHEDULE_CHECK_SERVER_STATUS,
  MANUALLY_CHECK_SERVER_STATUS,
  CHECK_SERVER_STATUS_START,
  CHECK_SERVER_STATUS_SUCCESS,
  CHECK_SERVER_STATUS_FAILURE,
  checkServerStatus
} from 'actions/serverStatus';

export const RETRY_TIME = 5000;
export const MAX_RETRY_TIME = 120000;

export default function *serverStatusWatcher() {
  while (true) { // eslint-disable-line no-constant-condition
    yield take(SCHEDULE_CHECK_SERVER_STATUS);
    yield* scheduleCheckServerStatus();
  }
}

export function *scheduleCheckServerStatus() {
  yield put(checkServerStatus(RETRY_TIME));
  let isCanceled = false;
  let attemptCount = 1;
  while (!isCanceled) {
    const {cancel, manuallyCheck} = yield race({
      cancel: take(UNSCHEDULE_CHECK_SERVER_STATUS),
      manuallyCheck: take(MANUALLY_CHECK_SERVER_STATUS),
      timeout: call(checkAfterDelay, attemptCount)
    });
    if (manuallyCheck) {
      yield put(checkServerStatus());
      yield take(CHECK_SERVER_STATUS_START);
      yield take([CHECK_SERVER_STATUS_START, CHECK_SERVER_STATUS_SUCCESS, CHECK_SERVER_STATUS_FAILURE]);
    } else {
      attemptCount += 1;
    }
    isCanceled = Boolean(cancel);
  }
}

export function *checkAfterDelay(attemptCount) {
  yield call(delay, getDelay(attemptCount));
  yield put(checkServerStatus(getDelay(attemptCount + 1)));
}

function getDelay(attemptCount) {
  return Math.min(attemptCount * RETRY_TIME, MAX_RETRY_TIME);
}
