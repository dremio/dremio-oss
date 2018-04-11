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
import { API_URL_V2 } from 'constants/Api';

import {makeUncachebleURL} from 'ie11.js';

import sourceSchema from 'dyn-load/schemas/source';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const LOAD_SOURCE_STARTED = 'LOAD_SOURCE_STARTED';
export const LOAD_SOURCE_SUCCESS = 'LOAD_SOURCE_SUCCESS';
export const LOAD_SOURCE_FAILURE = 'LOAD_SOURCE_FAILURE';

function fetchSourceData(href) {
  const resourcePath = href;
  const meta = { resourcePath };
  const uiPropsForEntity = [{key: 'resourcePath', value: resourcePath}];
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_SOURCE_STARTED, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_SOURCE_SUCCESS, sourceSchema, meta, uiPropsForEntity),
        { type: LOAD_SOURCE_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: makeUncachebleURL(`${API_URL_V2}${resourcePath}`)
    }
  };
}

export function loadSourceData(href) {
  return (dispatch) => {
    return dispatch(fetchSourceData(href));
  };
}
