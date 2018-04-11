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

import schemaUtils from 'utils/apiUtils/schemaUtils';
import { API_URL_V2 } from 'constants/Api';

import userSchema from 'schemas/user';
import {makeUncachebleURL} from 'ie11.js';


const USER_GET_START = 'USER_GET_START';
const USER_GET_SUCCESS = 'USER_GET_SUCCESS';
const USER_GET_FAILURE = 'USER_GET_FAILURE';

function fetchUser(value, meta = {}) {
//   let urlEnd = value;
//   if (typeof value === 'object') {
//     // todo: replace with URLSearchParams polyfill, DRY
//     urlEnd = '?' + Object.entries(value).map(
//       ([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`
//     ).join('&');
//   }
  const urlEnd = value.userName; // todo: update when server API updates

  return {
    [CALL_API]: {
      types: [
        {type: USER_GET_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(USER_GET_SUCCESS, userSchema, meta),
        {type: USER_GET_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: makeUncachebleURL(`${API_URL_V2}/user/${urlEnd}`)
    }
  };
}

export function loadUser() { // todo: audit uses of this call and switch to ids where possible (vs userName)
  return (dispatch) => {
    return dispatch(fetchUser(...arguments));
  };
}
