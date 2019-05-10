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
import { unauthorizedError, LOGIN_USER_FAILURE, noUsersError } from 'actions/account';
import { push } from 'react-router-redux';
import {get} from 'lodash/object';

import { LOGIN_PATH, getLoginUrl, SIGNUP_PATH } from 'routes';

export const UNAUTHORIZED_URL_PARAM = 'reason=401';

export const isUnauthorisedReason = (nextLocation) => {
  const search = get(nextLocation, 'search');
  return search && search.includes(UNAUTHORIZED_URL_PARAM);
};

function authMiddleware() {
  return (store) => next => action => {
    const payload = action.payload;
    if (action.error && payload) {
      // if the action is a login failure, we don't want to call logoutUser and instead let the login code handle the
      // LOGIN_USER_FAILURE action
      if (payload.status === 401 && action.type !== LOGIN_USER_FAILURE) {
        const atLogin = window.location.pathname === LOGIN_PATH;
        if (!atLogin) { // avoid pushing twice, resulting in a redirect URL *to* /login (and multiple history entries)
          next(push(`${getLoginUrl()}&${UNAUTHORIZED_URL_PARAM}`)); // eslint-disable-line callback-return
        }
        return next(unauthorizedError());
      }

      if (payload.status === 403 && payload.response && payload.response.errorMessage === 'No User Available') { // todo: use a key not a human message here
        const atSignup = window.location.pathname === SIGNUP_PATH;
        if (!atSignup) { // avoid multiple history entries
          next(push(SIGNUP_PATH)); // eslint-disable-line callback-return
        }
        return next(noUsersError()); // stop further handling and signal socket closing, etc
      }
    }
    return next(action);
  };
}
export default authMiddleware();
