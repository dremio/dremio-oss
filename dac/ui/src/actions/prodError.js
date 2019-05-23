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
import invariant from 'invariant';
import sentryUtil from '@app/utils/sentryUtil';

export const SHOW_PROD_ERROR = 'SHOW_PROD_ERROR';
export const HIDE_PROD_ERROR = 'HIDE_PROD_ERROR';

const e2eTestErrorMessage = 'JS_ERROR_OCCURRED';

const getError = (e) => {
  invariant(e, 'error must be provided');
  if (e instanceof Error) {
    return e;
  }
  if (e.message) {
    return new Error(e.message + '\n\n' + e.stack + '\n\n(non-Error instance)');
  }
  return new Error(e); // error components expect objects
};

export function showAppError(error) {
  // a signal for e2e tests
  console.error(e2eTestErrorMessage);
  return {
    type: SHOW_PROD_ERROR,
    error: getError(error),
    errorId: sentryUtil.getEventId()
  };
}

export function hideAppError() {
  return { type: HIDE_PROD_ERROR };
}
