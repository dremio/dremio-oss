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

import { APIV2Call } from '@app/core/APICall';

export const SCHEDULE_CHECK_SERVER_STATUS = 'SCHEDULE_CHECK_SERVER_STATUS';
export function scheduleCheckServerStatus(delay) {
  return {type: SCHEDULE_CHECK_SERVER_STATUS, meta: {delay}};
}

export const UNSCHEDULE_CHECK_SERVER_STATUS = 'UNSCHEDULE_CHECK_SERVER_STATUS';
export function unscheduleCheckServerStatus() {
  return {type: UNSCHEDULE_CHECK_SERVER_STATUS};
}

export const MANUALLY_CHECK_SERVER_STATUS = 'MANUALLY_CHECK_SERVER_STATUS';

export function manuallyCheckServerStatus() {
  return {type: MANUALLY_CHECK_SERVER_STATUS};
}

export const CHECK_SERVER_STATUS_VIEW_ID = 'CHECK_SERVER_STATUS_VIEW_ID';

export const CHECK_SERVER_STATUS_START = 'CHECK_SERVER_STATUS_START';
export const CHECK_SERVER_STATUS_SUCCESS = 'CHECK_SERVER_STATUS_SUCCESS';
export const CHECK_SERVER_STATUS_FAILURE = 'CHECK_SERVER_STATUS_FAILURE';

export function checkServerStatus(delay) {
  const meta = {viewId: CHECK_SERVER_STATUS_VIEW_ID, delay};

  const apiCall = new APIV2Call().path('server_status');

  return {
    [RSAA]: {
      types: [
        { type: CHECK_SERVER_STATUS_START, meta },
        { type: CHECK_SERVER_STATUS_SUCCESS, meta },
        { type: CHECK_SERVER_STATUS_FAILURE, meta }
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function serverUnavailable(status) {
  return {type: CHECK_SERVER_STATUS_SUCCESS, error: true, payload: Immutable.Map({status})};
}
