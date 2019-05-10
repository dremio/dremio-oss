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

import fileSchema from 'schemas/file';
import fileFormatSchema from 'schemas/fileFormat';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import addFileModalMapper from 'utils/mappers/addFileModalMapper';
import apiUtils from '@app/utils/apiUtils/apiUtils';

export const UPLOAD_FILE_REQUEST = 'UPLOAD_FILE_REQUEST';
export const UPLOAD_FILE_SUCCESS = 'UPLOAD_FILE_SUCCESS';
export const UPLOAD_FILE_FAILURE = 'UPLOAD_FILE_FAILURE';

function postUploadToPath(parentEntity, file, fileConfig, extension, viewId) {
  const formData = new FormData();
  // use specific filename or just name of file
  const uploadFileName = fileConfig.name || file.name;
  formData.append('file', file, uploadFileName);
  formData.append('fileConfig', JSON.stringify(fileConfig));
  formData.append('fileName', uploadFileName);
  const parentPath = parentEntity.getIn(['links', 'upload_start']);
  const meta = {viewId};
  return {
    [CALL_API]: {
      types: [
        {type: UPLOAD_FILE_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(UPLOAD_FILE_SUCCESS, fileSchema, meta),
        {type: UPLOAD_FILE_FAILURE, meta}
      ],
      method: 'POST',
      body: formData,
      endpoint: `${API_URL_V2}${parentPath}/?extension=${extension}`
    }
  };
}

export function uploadFileToPath(parentEntity, file, fileConfig, extension, viewId) {
  return (dispatch) => {
    return dispatch(postUploadToPath(parentEntity, file, fileConfig, extension, viewId));
  };
}

export const LOAD_FILE_FORMAT_REQUEST = 'LOAD_FILE_FORMAT_REQUEST';
export const LOAD_FILE_FORMAT_SUCCESS = 'LOAD_FILE_FORMAT_SUCCESS';
export const LOAD_FILE_FORMAT_FAILURE = 'LOAD_FILE_FORMAT_FAILURE';

function fetchFileFormat(file, viewId) {
  const meta = {
    parentEntityId: file.get('entityType'),
    parentEntityType: file.get('entityType'),
    viewId
  };
  return {
    [CALL_API]: {
      types: [
        {type: LOAD_FILE_FORMAT_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_FILE_FORMAT_SUCCESS, fileFormatSchema, meta),
        {type: LOAD_FILE_FORMAT_FAILURE, meta}],
      method: 'GET',
      endpoint: `${API_URL_V2}${file.getIn(['links', 'format'])}`
    }
  };
}

export function loadFileFormat(file, viewId) {
  return (dispatch) => {
    return dispatch(fetchFileFormat(file, viewId));
  };
}



export const FILE_FORMAT_PREVIEW_REQUEST = 'FILE_FORMAT_PREVIEW_REQUEST';
export const FILE_FORMAT_PREVIEW_SUCCESS = 'FILE_FORMAT_PREVIEW_SUCCESS';
export const FILE_FORMAT_PREVIEW_FAILURE = 'FILE_FORMAT_PREVIEW_FAILURE';

function fetchFileFormatPreview(urlPath, values, viewId) {
  const meta = {viewId};
  return {
    [CALL_API]: {
      types: [
        {type: FILE_FORMAT_PREVIEW_REQUEST, meta},
        {type: FILE_FORMAT_PREVIEW_SUCCESS, meta},
        {type: FILE_FORMAT_PREVIEW_FAILURE, meta}
      ],
      method: 'POST',
      body: JSON.stringify(values),
      headers: {
        'Content-Type': 'application/json',
        ...apiUtils.getJobDataNumbersAsStringsHeader()
      },
      endpoint: `${API_URL_V2}${urlPath}`
    }
  };
}

export function loadFilePreview(file, values, viewId) {
  return (dispatch) => {
    return dispatch(fetchFileFormatPreview(file.getIn(['links', 'format_preview']), values, viewId));
  };
}

export const RESET_FILE_FORMAT_PREVIEW = 'RESET_FILE_FORMAT_PREVIEW';
export function resetFileFormatPreview() {
  return {type: RESET_FILE_FORMAT_PREVIEW};
}

export const FILE_FORMAT_SAVE_REQUEST = 'FILE_FORMAT_SAVE_REQUEST';
export const FILE_FORMAT_SAVE_SUCCESS = 'FILE_FORMAT_SAVE_SUCCESS';
export const FILE_FORMAT_SAVE_FAILURE = 'FILE_FORMAT_SAVE_FAILURE';

function postFileFormat(fileFormat, values, viewId) {
  const resourcePath = fileFormat.getIn(['links', 'self']);
  const meta = {
    viewId,
    invalidateViewIds: ['HomeContents']
  };
  return {
    [CALL_API]: {
      types: [
        {type: FILE_FORMAT_SAVE_REQUEST, meta},
        {type: FILE_FORMAT_SAVE_SUCCESS, meta},
        {type: FILE_FORMAT_SAVE_FAILURE, meta}
      ],
      method: 'PUT',
      body: addFileModalMapper.unescapeFilePayload(values),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${resourcePath}`
    }
  };
}

export function saveFileFormat(fileFormat, values, viewId) {
  return (dispatch) => {
    return dispatch(postFileFormat(fileFormat, values, viewId));
  };
}



export const UPLOAD_FINISH_REQUEST = 'UPLOAD_FINISH_REQUEST';
export const UPLOAD_FINISH_SUCCESS = 'UPLOAD_FINISH_SUCCESS';
export const UPLOAD_FINISH_FAILURE = 'UPLOAD_FINISH_FAILURE';

function postUploadFinish(file, values, viewId) {
  const resourcePath = file.getIn(['links', 'upload_finish']);
  const meta = {
    viewId,
    invalidateViewIds: ['HomeContents']
  };
  return {
    [CALL_API]: {
      types: [
        {type: UPLOAD_FINISH_REQUEST, meta},
        {type: UPLOAD_FINISH_SUCCESS, meta},
        {type: UPLOAD_FINISH_FAILURE, meta}
      ],
      method: 'POST',
      body: addFileModalMapper.unescapeFilePayload(values),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${resourcePath}`
    }
  };
}

export function uploadFinish(file, values, viewId) {
  return (dispatch) => {
    return dispatch(postUploadFinish(file, values, viewId));
  };
}

export const UPLOAD_CANCEL_REQUEST = 'UPLOAD_CANCEL_REQUEST';
export const UPLOAD_CANCEL_SUCCESS = 'UPLOAD_CANCEL_SUCCESS';
export const UPLOAD_CANCEL_FAILURE = 'UPLOAD_CANCEL_FAILURE';

function postUploadCancel(file) {
  const resourcePath = file.getIn(['links', 'upload_cancel']);
  return {
    [CALL_API]: {
      types: [
        {type: UPLOAD_CANCEL_REQUEST},
        {type: UPLOAD_CANCEL_SUCCESS},
        {type: UPLOAD_CANCEL_FAILURE}
      ],
      method: 'POST',
      body: addFileModalMapper.unescapeFilePayload(file.get('fileFormat').toJS()),
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${resourcePath}`
    }
  };
}

export function uploadCancel(file) {
  return (dispatch) => {
    return dispatch(postUploadCancel(file));
  };
}
