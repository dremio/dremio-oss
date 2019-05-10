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

import { makeUncachebleURL } from 'ie11.js';

import folderSchema from 'schemas/folder';
import datasetSchema from 'schemas/dataset';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import actionUtils from 'utils/actionUtils/actionUtils';

import { constructFullPathAndEncode } from 'utils/pathUtils';

import { VIEW_ID as HOME_CONTENTS_VIEW_ID } from 'pages/HomePage/subpages/HomeContents';

export const ADD_FOLDER_START = 'ADD_FOLDER_START';
export const ADD_FOLDER_SUCCESS = 'ADD_FOLDER_SUCCESS';
export const ADD_FOLDER_FAILURE = 'ADD_FOLDER_FAILURE';

function fetchAddNewFolder(parentEntity, parentType, name) {
  const parentPath = parentEntity.getIn(['links', 'self']);
  const resourcePath = parentType === 'folder' ? `${parentPath}`
    : `${parentPath}/folder/`;
  const meta = { resourcePath, invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };
  return {
    [CALL_API]: {
      types: [
        {
          type: ADD_FOLDER_START,
          meta
        },
        schemaUtils.getSuccessActionTypeWithSchema(ADD_FOLDER_SUCCESS, folderSchema, meta),
        {
          type: ADD_FOLDER_FAILURE,
          meta
        }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        name
      }),

      endpoint: makeUncachebleURL(`${API_URL_V2}${resourcePath}`)
    }
  };
}

export function addNewFolderForSpace(parentEntity, parentType, name) {
  return (dispatch) => {
    return dispatch(fetchAddNewFolder(parentEntity, parentType, name));
  };
}

export const REMOVE_SPACE_FOLDER_START = 'REMOVE_SPACE_FOLDER_START';
export const REMOVE_SPACE_FOLDER_SUCCESS = 'REMOVE_SPACE_FOLDER_SUCCESS';
export const REMOVE_SPACE_FOLDER_FAILURE = 'REMOVE_SPACE_FOLDER_FAILURE';

function fetchRemoveFolder(folder) {
  const resourcePath = folder.getIn(['links', 'self']);
  const meta = {
    resourcePath,
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID]
  };
  return {
    [CALL_API]: {
      types: [
        {
          type: REMOVE_SPACE_FOLDER_START,
          meta
        },
        {
          type: REMOVE_SPACE_FOLDER_SUCCESS,
          meta: {...meta, success: true}
        },
        {
          type: REMOVE_SPACE_FOLDER_FAILURE,
          meta: {
            ...meta,
            notification: {
              message: la('There was an error removing the folder.'),
              level: 'error'
            }
          }
        }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}${resourcePath}?version=${folder.get('version')}`
    }
  };
}

export function removeSpaceFolder(folder) {
  return (dispatch) => {
    return dispatch(fetchRemoveFolder(folder));
  };
}

export const REMOVE_FILE_START = 'REMOVE_FILE_START';
export const REMOVE_FILE_SUCCESS = 'REMOVE_FILE_SUCCESS';
export const REMOVE_FILE_FAILURE = 'REMOVE_FILE_FAILURE';

function fetchRemoveFile(file) {
  const resourcePath = file.getIn(['links', 'self']);
  const meta = {
    resourcePath,
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID]
  };
  const errorMessage = la('There was an error removing the file.');
  return {
    [CALL_API]: {
      types: [
        {
          type: REMOVE_FILE_START,
          meta
        },
        {
          type: REMOVE_FILE_SUCCESS,
          meta: {...meta, success: true}
        },
        {
          type: REMOVE_FILE_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}${resourcePath}?version=${file.getIn(['fileFormat', 'version'])}`
    }
  };
}

export function removeFile(file) {
  return (dispatch) => {
    return dispatch(fetchRemoveFile(file));
  };
}

export const REMOVE_FILE_FORMAT_START = 'REMOVE_FILE_FORMAT_START';
export const REMOVE_FILE_FORMAT_SUCCESS = 'REMOVE_FILE_FORMAT_SUCCESS';
export const REMOVE_FILE_FORMAT_FAILURE = 'REMOVE_FILE_FORMAT_FAILURE';

function fetchRemoveFileFormat(file) {
  const meta = {
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID]
  };
  const errorMessage = la('There was an error removing the format for the file.');
  const entityRemovePaths = [['fileFormat', file.getIn(['fileFormat', 'id'])]];
  return {
    [CALL_API]: {
      types: [
        { type: REMOVE_FILE_FORMAT_START, meta},
        { type: REMOVE_FILE_FORMAT_SUCCESS, meta: {...meta, success: true, entityRemovePaths}},
        {
          type: REMOVE_FILE_FORMAT_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}${file.getIn(['links', 'delete_format'])}`
    }
  };
}

export function removeFileFormat(file) {
  return (dispatch) => {
    return dispatch(fetchRemoveFileFormat(file));
  };
}


export const RENAME_SPACE_DATASET_START = 'RENAME_SPACE_DATASET_START';
export const RENAME_SPACE_DATASET_SUCCESS = 'RENAME_SPACE_DATASET_SUCCESS';
export const RENAME_SPACE_DATASET_FAILURE = 'RENAME_SPACE_DATASET_FAILURE';

function fetchRenameDataset(dataset, newName) {
  const href = constructFullPathAndEncode(dataset.get('fullPathList'));
  const encodedNewName = encodeURIComponent(newName);
  const meta = { newName, invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };
  return {
    [CALL_API]: {
      types: [
        {
          type: RENAME_SPACE_DATASET_START,
          meta
        },
        schemaUtils.getSuccessActionTypeWithSchema(RENAME_SPACE_DATASET_SUCCESS, datasetSchema, meta),
        {
          type: RENAME_SPACE_DATASET_FAILURE,
          meta
        }
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}/dataset/${href}/rename?renameTo=${encodedNewName}`
    }
  };
}

export function renameSpaceDataset(dataset, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameDataset(dataset, newName));
  };
}

export const REMOVE_DATASET_START = 'REMOVE_DATASET_START';
export const REMOVE_DATASET_SUCCESS = 'REMOVE_DATASET_SUCCESS';
export const REMOVE_DATASET_FAILURE = 'REMOVE_DATASET_FAILURE';

function fetchRemoveDataset(dataset) {
  const href = dataset.get('resourcePath');
  const meta = {
    name: dataset.get('name'),
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID]
  };
  const notification = {
    message: la('Successfully removed.'),
    level: 'success'
  };
  const errorMessage = la('There was an error removing the dataset.');
  return {
    [CALL_API]: {
      types: [
        {
          type: REMOVE_DATASET_START, meta
        },
        {
          type: REMOVE_DATASET_SUCCESS, meta: {...meta, success: true, notification}
        },
        {
          type: REMOVE_DATASET_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
      ],
      method: 'DELETE',
      endpoint: `${API_URL_V2}${href}?savedTag=${dataset.getIn(['datasetConfig', 'savedTag'])}`    }
  };
}

export function removeDataset(dataset) {
  return (dispatch) => {
    return dispatch(fetchRemoveDataset(dataset));
  };
}

export const RENAME_FOLDER_START = 'RENAME_FOLDER_START';
export const RENAME_FOLDER_SUCCESS = 'RENAME_FOLDER_SUCCESS';
export const RENAME_FOLDER_FAILURE = 'RENAME_FOLDER_FAILURE';

function fetchRenameFolder(folder, newName) {
  const meta = { invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };
  return {
    [CALL_API]: {
      types: [
        {
          type: RENAME_FOLDER_START,
          meta
        },
        {
          type: RENAME_FOLDER_SUCCESS,
          meta
        },
        {
          type: RENAME_FOLDER_FAILURE,
          meta
        }
      ],
      method: 'POST',
      endpoint: `${API_URL_V2}${folder.getIn(['links', 'rename'])}?renameTo=${newName}`
    }
  };
}

export function renameFolder(folder, oldName, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameFolder(folder, oldName, newName));
  };
}

export const LOAD_DEPENDENT_DATASETS_STARTED = 'LOAD_DEPENDENT_DATASETS_STARTED';
export const LOAD_DEPENDENT_DATASETS_SUCCESS = 'LOAD_DEPENDENT_DATASETS_SUCCESS';
export const LOAD_DEPENDENT_DATASETS_FAILURE = 'LOAD_DEPENDENT_DATASETS_FAILURE';

function fetchDependentDatasets(fullPath) {
  const href = constructFullPathAndEncode(fullPath);
  return {
    [CALL_API]: {
      types: [LOAD_DEPENDENT_DATASETS_STARTED, LOAD_DEPENDENT_DATASETS_SUCCESS, LOAD_DEPENDENT_DATASETS_FAILURE],
      method: 'GET',
      endpoint: `${API_URL_V2}/dataset/${href}/descendants`
    }
  };
}

export function loadDependentDatasets(fullPath) {
  return (dispatch) => {
    return dispatch(fetchDependentDatasets(fullPath));
  };
}


export const LOAD_PARENTS_START = 'LOAD_PARENTS_START';
export const LOAD_PARENTS_SUCCESS = 'LOAD_PARENTS_SUCCESS';
export const LOAD_PARENTS_FAILURE = 'LOAD_PARENTS_FAILURE';

function fetchParents(fullPath, version, viewId) {
  const href = constructFullPathAndEncode(fullPath);
  const meta = {viewId};
  return {
    [CALL_API]: {
      types: [{
        type: LOAD_PARENTS_START, meta
      }, {
        type: LOAD_PARENTS_SUCCESS, meta
      }, {
        type: LOAD_PARENTS_FAILURE, meta
      }],
      method: 'GET',
      endpoint: `${API_URL_V2}/dataset/${href}/version/${version}/parents`
    }
  };
}

export function loadParents() {
  return (dispatch) => dispatch(fetchParents(...arguments));
}
