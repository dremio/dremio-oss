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
import {
  LOGIN_USER_FAILURE,
  CHECK_USER_FAILURE,
  noUsersError,
  unauthorizedError,
} from "actions/account";
import { push } from "react-router-redux";
import { get } from "lodash/object";

import { LOGIN_PATH, getLoginUrl, SIGNUP_PATH } from "#oss/sagas/loginLogout";

export const UNAUTHORIZED_URL_PARAM = "reason=401";

export const isUnauthorisedReason = (nextLocation) => {
  const search = get(nextLocation, "search");
  return search && search.includes(UNAUTHORIZED_URL_PARAM);
};

const isAuthCheckAction = (action) =>
  action &&
  (action.type === LOGIN_USER_FAILURE || action.type === CHECK_USER_FAILURE);

export function handleUnauthorized(payload, next, action) {
  if (payload.status === 401 && !isAuthCheckAction(action)) {
    const atLogin = window.location.pathname === LOGIN_PATH;
    next(unauthorizedError());
    if (!atLogin) {
      setTimeout(() => {
        window.location.assign(
          `${getLoginUrl(true)}&${UNAUTHORIZED_URL_PARAM}`,
        );
      }, 0);
    }
  }

  if (
    payload.status === 403 &&
    payload.response &&
    payload.response.errorMessage === "No User Available"
  ) {
    // todo: use a key not a human message here
    const atSignup = window.location.pathname === SIGNUP_PATH;
    if (!atSignup) {
      // avoid multiple history entries
      next(push(SIGNUP_PATH));
    }
    return next(noUsersError()); // stop further handling and signal socket closing, etc
  }
}

function authMiddleware() {
  return () => (next) => (action) => {
    const payload = action.payload;
    // TODO put that logic to a saga
    if (action.error && payload) {
      const resultAction = handleUnauthorized(payload, next, action);
      if (resultAction) return resultAction;
    }
    return next(action);
  };
}
export default authMiddleware();
