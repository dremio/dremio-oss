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

import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';
import APICall from '@app/core/APICall';

function headerMiddleware() {
  return () => next => action => {
    const apiCall = action[RSAA];

    if (apiCall) {
      // create a new action and remove isFileUpload if present
      const newAction = {
        [RSAA]: apiCall
      };
      const method = apiCall.method;
      const token = localStorageUtils.getAuthToken();

      if (apiCall.endpoint instanceof APICall) {
        apiCall.endpoint = apiCall.endpoint.toString();
      }

      if (method === 'GET' || method === 'POST' || method === 'PUT' || method === 'DELETE') {
        const { isFileUpload } = action;
        const defaultHeaders = {
          Authorization: token,
          // for file upload case leave headers empty and let a browser to set a content type
          ...(isFileUpload ? null : { 'Content-Type': 'application/json' })
        };
        apiCall.headers = { // avoid mutating original
          ...defaultHeaders,
          ...apiCall.headers
        };

        return next(newAction);
      }
    }

    return next(action);
  };
}
export default headerMiddleware();
