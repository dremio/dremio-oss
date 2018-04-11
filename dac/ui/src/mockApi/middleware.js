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
import isPlainObject from 'lodash/isPlainObject';
import { RequestError } from 'redux-api-middleware/lib/errors';
import { normalizeTypeDescriptors, actionWith } from 'redux-api-middleware/lib/util';

import CALL_MOCK_API from './CALL_MOCK_API';

function getMockResponse(action) {
  const callAPI = action[CALL_MOCK_API];
  const defaultHeaders = new Headers();
  defaultHeaders.append('Content-Type', 'application/json');

  const res = typeof callAPI.mockResponse === 'function' ? callAPI.mockResponse(action) : callAPI.mockResponse;

  return {
    ok: true,
    headers: defaultHeaders,
    json: () => new Promise((resolve) => resolve(res.responseJSON)),
    ...res
  };
}

function isMockRSAA(action) {
  return isPlainObject(action) && action.hasOwnProperty(CALL_MOCK_API);
}

function validateMockRSAA(action) {
  const validationErrors = [];
  for (const key in action) {
    if (key !== [CALL_MOCK_API]) {
      validationErrors.push(`Invalid root key: ${key}`);
    }
  }

  const callAPI = action[CALL_MOCK_API];
  if (!isPlainObject(callAPI)) {
    validationErrors.push('[CALL_MOCK_API] property must be a plain JavaScript object');
  }

  const { mockResponse } = callAPI;
  if (typeof mockResponse === 'undefined') {
    validationErrors.push('[CALL_MOCK_API] must have a mockResponse property');
  } else if (isPlainObject(mockResponse)) {
    if (mockResponse.ok !== false &&  typeof mockResponse.responseJSON === 'undefined') {
      validationErrors.push(
          '[CALL_MOCK_API] must have a mockResponse.responseJSON if mockResponse.ok is true or undefined');
    }
  } else if (typeof mockResponse !== 'function') {
    validationErrors.push(
        'mockResponse must be a plain js object or a function that resolves to one');
  }

  return validationErrors;
}

/**
 * A Redux middleware that processes RSAA actions.
 *
 * @type {ReduxMiddleware}
 * @access public
 */
export default function mockApiMiddleware({ dispatch, getState }) {
  return (next) => async (action) => {
    // Do not process actions without a [CALL_API] property
    if (!isMockRSAA(action)) {
      return next(action);
    }

    // Try to dispatch an error request FSA for invalid RSAAs
    const validationErrors = validateMockRSAA(action);
    if (validationErrors.length) {
      throw validationErrors;
    }

    // Parse the validated RSAA action
    const callAPI = action[CALL_MOCK_API];
    let { endpoint, headers } = callAPI;
    const { bailout, types } = callAPI;
    const [requestType, successType, failureType] = normalizeTypeDescriptors(types);

    // Should we bail out?
    try {
      if ((typeof bailout === 'boolean' && bailout) ||
          (typeof bailout === 'function' && bailout(getState()))) {
        return;
      }
    } catch (e) {
      return next({
        ...requestType,
        payload: new RequestError('[CALL_API].bailout function failed'),
        error: true
      });
    }

    // Process [CALL_API].endpoint function
    if (typeof endpoint === 'function') {
      try {
        endpoint = endpoint(getState());
      } catch (e) {
        return next({
          ...requestType,
          payload: new RequestError('[CALL_API].endpoint function failed'),
          error: true
        });
      }
    }

    // Process [CALL_API].headers function
    if (typeof headers === 'function') {
      try {
        headers = headers(getState());
      } catch (e) {
        return next({
          ...requestType,
          payload: new RequestError('[CALL_API].headers function failed'),
          error: true
        });
      }
    }

    // We can now dispatch the request FSA
    dispatch(requestType);

    // get fake response
    const res = getMockResponse(action);

    await sleep(1000);

    // Process the server response
    if (res.ok) {
      return next(await actionWith(
        successType,
        [action, getState(), res]
      ));
    }
    return next(await actionWith(
      {
        ...failureType,
        error: true
      },
      [action, getState(), res]
    ));
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
