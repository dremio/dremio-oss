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
import schemaUtils from 'utils/apiUtils/schemaUtils';
import datasetAccelerationSettingsSchema from 'schemas/datasetAccelerationSettings';
import { constructFullPathAndEncode, constructFullPath } from 'utils/pathUtils';

export const LOAD_DATASET_ACCELERATION_SETTINGS_START = 'LOAD_DATASET_ACCELERATION_SETTINGS_START';
export const LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS = 'LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS';
export const LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE = 'LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE';

export function loadDatasetAccelerationSettings(fullPathList, viewId) {
  const encodedDatasetPath = constructFullPathAndEncode(fullPathList);
  const meta = { viewId };
  // TODO: this is a workaround for accelerationSettings not having its own id
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_DATASET_ACCELERATION_SETTINGS_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS,
          datasetAccelerationSettingsSchema, meta,
          'datasetResourcePath', constructFullPath(fullPathList)
        ),
        {type: LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/dataset/${encodedDatasetPath}/acceleration/settings`
    }
  };
}

export const UPDATE_DATASET_ACCELERATION_SETTINGS_START = 'UPDATE_DATASET_ACCELERATION_SETTINGS_START';
export const UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS = 'UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS';
export const UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE = 'UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE';

export function updateDatasetAccelerationSettings(fullPathList, form) {
  const datasetPath = constructFullPathAndEncode(fullPathList);
  return {
    [CALL_API]: {
      types: [
        UPDATE_DATASET_ACCELERATION_SETTINGS_START,
        UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS,
        UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE
      ],
      method: 'PUT',
      body: JSON.stringify(form),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}/dataset/${datasetPath}/acceleration/settings`
    }
  };
}
