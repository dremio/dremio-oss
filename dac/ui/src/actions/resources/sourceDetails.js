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

import sourceSchema from 'dyn-load/schemas/source';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { APIV2Call } from '@app/core/APICall';

export const LOAD_SOURCE_STARTED = 'LOAD_SOURCE_STARTED';
export const LOAD_SOURCE_SUCCESS = 'LOAD_SOURCE_SUCCESS';
export const LOAD_SOURCE_FAILURE = 'LOAD_SOURCE_FAILURE';

function fetchSourceData(href) {
  const resourcePath = href;
  const meta = { resourcePath };
  const uiPropsForEntity = [{key: 'resourcePath', value: resourcePath}];

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .uncachable();

  return {
    [RSAA]: {
      types: [
        { type: LOAD_SOURCE_STARTED, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_SOURCE_SUCCESS, sourceSchema, meta, uiPropsForEntity),
        { type: LOAD_SOURCE_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadSourceData(href) {
  return (dispatch) => {
    return dispatch(fetchSourceData(href));
  };
}
