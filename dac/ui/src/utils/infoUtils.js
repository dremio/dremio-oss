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

import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';
import { log } from '@app/utils/logger';
import APICall from '@app/core/APICall';

export const getClusterInfo = async () => {
  const apiCall = new APICall().path('info');
  const method = 'GET';
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': localStorageUtils.getAuthToken()
  };
  const apiResponse = await fetch(apiCall, {method, headers});
  if (apiResponse.ok) {
    const responseJson = await apiResponse.json();
    return responseJson;
  }
  const error = await apiResponse.text();
  log('error', error);
  return {};
};
