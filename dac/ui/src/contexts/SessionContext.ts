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
  getSessionContext as baseGetSessionContext,
  setSessionContext,
} from "dremio-ui-common/contexts/SessionContext.js";

const SESSION_KEY = "user";

const getSessionIdentifier = (): string | null => {
  const user = window.localStorage.getItem(SESSION_KEY);

  if (!user) {
    return null;
  }

  try {
    return JSON.parse(user).token as string;
  } catch (e) {
    return null;
  }
};

const sessionIsValid = (): boolean => {
  const sessionIdentifier = getSessionIdentifier();

  // return sessionIdentifier !== null && !jwtIsExpired(sessionIdentifier);
  return sessionIdentifier !== null;
};

const handleLogout = () => {
  window.localStorage.removeItem(SESSION_KEY);

  const loginUrl = new URL("/login", window.location.origin);
  loginUrl.searchParams.append(
    "redirect",
    window.location.href.slice(window.location.origin.length)
  );
  window.location.assign(loginUrl);
};

const sessionContext = {
  getSessionIdentifier,
  sessionIsValid,
  handleInvalidSession: () => {
    handleLogout();
  },
  handleLogout,
};

setSessionContext(sessionContext);

export const getSessionContext =
  baseGetSessionContext as () => typeof sessionContext;
