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
import { CALL_API } from 'redux-api-middleware';

import localStorageUtils from 'utils/storageUtils/localStorageUtils';

function headerMiddleware() {
  return () => next => action => {

    if (action[CALL_API]) {
      let headers;
      if (action[CALL_API].headers) {
        headers = action[CALL_API].headers = {...action[CALL_API].headers}; // avoid mutating original
      } else {
        headers = action[CALL_API].headers = {};
      }

      const method = action[CALL_API].method;
      const token = localStorageUtils.getAuthToken();
      if (method === 'GET' || method === 'POST' || method === 'PUT' || method === 'DELETE') {
        headers.Authorization = headers.Authorization || token;

        return next(action);
      }
    }

    return next(action);
  };
}
export default headerMiddleware();
