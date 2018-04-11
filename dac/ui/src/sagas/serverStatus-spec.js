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
  // SCHEDULE_CHECK_SERVER_STATUS,
  UNSCHEDULE_CHECK_SERVER_STATUS,
  MANUALLY_CHECK_SERVER_STATUS,
  CHECK_SERVER_STATUS_START,
  CHECK_SERVER_STATUS_SUCCESS,
  CHECK_SERVER_STATUS_FAILURE,
  checkServerStatus
} from 'actions/serverStatus';

const getRace = (attemptCount) => race({
  cancel: take(UNSCHEDULE_CHECK_SERVER_STATUS),
  manuallyCheck: take(MANUALLY_CHECK_SERVER_STATUS),
  timeout: call(checkAfterDelay, attemptCount)
});

import { RETRY_TIME, scheduleCheckServerStatus, checkAfterDelay } from './serverStatus';


describe('serverStatus saga', () => {
  describe('checkAfterDelay', () => {
    let generator;
    beforeEach(() => {
      generator = checkAfterDelay(1);
    });

    it('should yield delay, then checkServerStatus', () => {
      let next = generator.next();
      expect(next.value).to.eql(call(delay, RETRY_TIME));
      next = generator.next();
      expect(next.value).to.eql(put(checkServerStatus()));
    });
  });

  describe('scheduleCheckServerStatus', () => {
    let generator;
    beforeEach(() => {
      generator = scheduleCheckServerStatus();
    });

    it('should yield checkServerStatus, then race', () => {
      let next = generator.next();
      expect(next.value).to.eql(put(checkServerStatus(RETRY_TIME)));
      next = generator.next();
      expect(next.value).to.eql(getRace(1));
    });

    it('should stop when canceled', () => {
      generator.next(); // checkServerStatus,
      generator.next(); // race
      const next = generator.next({cancel: true});
      expect(next.done).to.be.true;
    });

    it('should attempt again after delay and increment attemptCount', () => {
      generator.next(); // checkServerStatus,
      generator.next(); // race
      const next = generator.next({});
      expect(next.value).to.eql(getRace(2));
    });

    it('should checkServerStatus and wait on manual check, not increment attemptCount', () => {
      generator.next(); // checkServerStatus,
      generator.next(); // race
      let next = generator.next({manuallyCheck: true});
      expect(next.value).to.eql(put(checkServerStatus()));
      next = generator.next();
      expect(next.value).to.eql(take(CHECK_SERVER_STATUS_START));
      next = generator.next();
      expect(next.value).to.eql(take([
        CHECK_SERVER_STATUS_START, CHECK_SERVER_STATUS_SUCCESS, CHECK_SERVER_STATUS_FAILURE
      ]));
      next = generator.next();
      expect(next.value).to.eql(getRace(1));
    });
  });
});
