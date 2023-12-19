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

import { setApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { getSessionContext } from "dremio-ui-common/contexts/SessionContext.js";
import { appFetch } from "dremio-ui-common/utilities/appFetch.js";

const ctx = {
  fetch: (input: RequestInfo | URL, init: RequestInit = {}) => {
    const sessionIdentifier = getSessionContext().getSessionIdentifier();
    if (!sessionIdentifier) {
      getSessionContext().handleInvalidSession();
      return new Promise(() => {});
    }

    return appFetch(input, {
      ...init,
      headers: {
        Authorization: `Bearer ${sessionIdentifier}`,
        ...init.headers,
      },
    });
  },
  createApiV2Url:
    (config: {} = {}) =>
    (path: string): URL => {
      const basePath = "/apiv2";
      return new URL(`${basePath}/${path}`, window.location.origin);
    },
  createApiV3Url:
    (config: {} = {}) =>
    (path: string): URL => {
      const basePath = "/api/v3";
      return new URL(`${basePath}/${path}`, window.location.origin);
    },
  createSonarUrl: (path: string): URL => {
    const basePath = "/apiv2";
    return new URL(`${basePath}/${path}`, window.location.origin);
  },
  doubleEncodeJsonParam: false,
};

setApiContext(ctx as any);

export const getApiContext = () => ctx;
