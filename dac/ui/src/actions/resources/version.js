/*
 * Copyright (C) 2017 Dremio Corporation
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
import { API_URL_V2 } from 'constants/Api';
import schema from 'schemas/version';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const LOAD_VERSION_START = 'LOAD_VERSION_START';
export const LOAD_VERSION_SUCCESS = 'LOAD_VERSION_SUCCESS';
export const LOAD_VERSION_FAILURE = 'LOAD_VERSION_FAILURE';

function fetchVersion(meta) {
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_VERSION_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_VERSION_SUCCESS, schema, meta),
        {type: LOAD_VERSION_FAILURE, meta}
      ],
      method: 'GET',
      headers: { 'Accept': 'application/json' }, // todo: BE fix for this
      endpoint: `${API_URL_V2}/version`
    }
  };
}

export function loadVersion() {
  return (dispatch) => {
    return dispatch(fetchVersion(...arguments));
  };
}
