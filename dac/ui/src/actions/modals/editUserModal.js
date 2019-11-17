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

import { API_URL_V3 } from '@app/constants/Api';
import { makeUncachebleURL } from 'ie11.js';
import { USERS_VIEW_ID } from '@app/actions/admin';

const v3ApiMigrationSuffix = '_API_V3'; // todo get rid of that suffix, when all UI would be migrated to v3 api
export const USER_GET_START = 'USER_GET_START' + v3ApiMigrationSuffix;
export const USER_GET_SUCCESS = 'USER_GET_SUCCESS' + v3ApiMigrationSuffix;
export const USER_GET_FAILURE = 'USER_GET_FAILURE' + v3ApiMigrationSuffix;

export function loadUser(userId) { // todo: audit uses of this call and switch to ids where possible (vs userName)
  return {
    [RSAA]: {
      types: [
        USER_GET_START,
        USER_GET_SUCCESS,
        USER_GET_FAILURE
      ],
      method: 'GET',
      endpoint: makeUncachebleURL(`${API_URL_V3}/user/${encodeURIComponent(userId)}`)
    }
  };
}


export const EDIT_USER_START = 'EDIT_USER_START';
export const EDIT_USER_SUCCESS = 'EDIT_USER_SUCCESS';
export const EDIT_USER_FAILURE = 'EDIT_USER_FAILURE';

export function editUser({
  id: userId, // empty for a new user
  ...values
}) {
  const isNewUser = !userId;
  const meta = {
    invalidateViewIds: [USERS_VIEW_ID],
    notification: {
      message: la('Successfully updated.'),
      level: 'success'
    }
  };
  return {
    [RSAA]: {
      types: [
        EDIT_USER_START,
        { type: EDIT_USER_SUCCESS, meta },
        EDIT_USER_FAILURE
      ],
      method: isNewUser ? 'POST' : 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(values),
      endpoint: `${API_URL_V3}/user/${isNewUser ? '' : encodeURIComponent(userId)}`
    }
  };
}
