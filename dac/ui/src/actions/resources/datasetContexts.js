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

import datasetContextSchema from 'schemas/v2/datasetContext';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { APIV2Call } from '@app/core/APICall';

export const LOAD_DATASET_CONTEXT_VIEW_ID = 'LOAD_DATASET_CONTEXT_VIEW_ID';

export const LOAD_DATASET_CONTEXT_STARTED = 'LOAD_DATASET_CONTEXT_STARTED';
export const LOAD_DATASET_CONTEXT_SUCCESS = 'LOAD_DATASET_CONTEXT_SUCCESS';
export const LOAD_DATASET_CONTEXT_FAILURE = 'LOAD_DATASET_CONTEXT_FAILURE';

function fetchDatasetContext(entity) {
  const href = entity.getIn(['links', 'context']);
  const meta = {viewId: LOAD_DATASET_CONTEXT_VIEW_ID};

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_DATASET_CONTEXT_STARTED, meta },
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_DATASET_CONTEXT_SUCCESS, datasetContextSchema, meta),
        { type: LOAD_DATASET_CONTEXT_FAILURE, meta }
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadDatasetContext(dataset) {
  return (dispatch) => {
    return dispatch(fetchDatasetContext(dataset));
  };
}
