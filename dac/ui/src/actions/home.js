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

import folderSchema from 'schemas/folder';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const CONVERT_FOLDER_TO_DATASET_START = 'CONVERT_FOLDER_TO_DATASET_START';
export const CONVERT_FOLDER_TO_DATASET_SUCCESS = 'CONVERT_FOLDER_TO_DATASET_SUCCESS';
export const CONVERT_FOLDER_TO_DATASET_FAILURE = 'CONVERT_FOLDER_TO_DATASET_FAILURE';

function fetchConvertFolder({folder, values, viewId}) {
  const meta = {viewId, invalidateViewIds: ['HomeContents']};
  return {
    [CALL_API]: {
      types: [
        CONVERT_FOLDER_TO_DATASET_START,
        schemaUtils.getSuccessActionTypeWithSchema(CONVERT_FOLDER_TO_DATASET_SUCCESS, folderSchema, meta),
        {type: CONVERT_FOLDER_TO_DATASET_FAILURE, meta}
      ],
      method: 'PUT',
      body: JSON.stringify(values),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${folder.getIn(['links', 'format'])}`
    }
  };
}

export function convertFolderToDataset({folder, values, viewId}) {
  return (dispatch) => {
    return dispatch(fetchConvertFolder({folder, values, viewId}));
  };
}


export const CONVERT_DATASET_TO_FOLDER_START = 'CONVERT_DATASET_TO_FOLDER_START';
export const CONVERT_DATASET_TO_FOLDER_SUCCESS = 'CONVERT_DATASET_TO_FOLDER_SUCCESS';
export const CONVERT_DATASET_TO_FOLDER_FAILURE = 'CONVERT_DATASET_TO_FOLDER_FAILURE';

function fetchConvertDataset(entity, viewId) {
  const meta = {viewId, folderId: entity.get('id')};
  return {
    [CALL_API]: {
      types: [
        {type:CONVERT_DATASET_TO_FOLDER_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(CONVERT_DATASET_TO_FOLDER_SUCCESS, folderSchema, meta),
        CONVERT_DATASET_TO_FOLDER_FAILURE
      ],
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${entity.getIn(['links', 'delete_format'])}`
    }
  };
}

export function convertDatasetToFolder(entity, viewId) {
  return (dispatch) => {
    return dispatch(fetchConvertDataset(entity, viewId));
  };
}
