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
import { datasetAccelerationSchema } from 'schemas/acceleration';
import {makeUncachebleURL} from 'ie11.js';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const GET_DATASET_ACCELERATION_START = 'GET_DATASET_ACCELERATION_START';
export const GET_DATASET_ACCELERATION_SUCCESS = 'GET_DATASET_ACCELERATION_SUCCESS';
export const GET_DATASET_ACCELERATION_FAILURE = 'GET_DATASET_ACCELERATION_FAILURE';

export const getDatasetAccelerationRequest = (fullPath) => ({
  [CALL_API]: {
    types: [
      GET_DATASET_ACCELERATION_START,
      schemaUtils.getSuccessActionTypeWithSchema(
        GET_DATASET_ACCELERATION_SUCCESS, datasetAccelerationSchema, {}, 'fullPath', fullPath
      ),
      GET_DATASET_ACCELERATION_FAILURE
    ],
    method: 'GET',
    endpoint: makeUncachebleURL(`${API_URL_V2}/dataset/${fullPath}/vote`)
  }
});
