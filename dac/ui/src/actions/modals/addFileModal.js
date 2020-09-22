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

import fileSchema from 'schemas/file';
import fileFormatSchema from 'schemas/fileFormat';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import addFileModalMapper from 'utils/mappers/addFileModalMapper';
import apiUtils from '@app/utils/apiUtils/apiUtils';
import { getHomeEntity } from '@app/selectors/home';
import { APIV2Call } from '@app/core/APICall';

export const UPLOAD_FILE_REQUEST = 'UPLOAD_FILE_REQUEST';
export const UPLOAD_FILE_SUCCESS = 'UPLOAD_FILE_SUCCESS';
export const UPLOAD_FILE_FAILURE = 'UPLOAD_FILE_FAILURE';

export const uploadFileToPath = (file, fileConfig, extension, viewId) => (dispatch, getState) => {
  const state = getState();
  const parentEntity = getHomeEntity(state);
  const formData = new FormData();
  // use specific filename or just name of file
  const uploadFileName = fileConfig.name || file.name;
  formData.append('file', file, uploadFileName);
  formData.append('fileConfig', JSON.stringify(fileConfig));
  formData.append('fileName', uploadFileName);
  const parentPath = parentEntity.getIn(['links', 'upload_start']);
  const meta = {viewId};

  const apiCall = new APIV2Call()
    .paths(parentPath)
    .params({extension});

  return dispatch({
    isFileUpload: true, // see headerMiddleware.js
    [RSAA]: {
      types: [
        {type: UPLOAD_FILE_REQUEST, meta},
        schemaUtils.getSuccessActionTypeWithSchema(UPLOAD_FILE_SUCCESS, fileSchema, meta),
        {type: UPLOAD_FILE_FAILURE, meta}
      ],
      method: 'POST',
      body: formData,
      endpoint: apiCall
    }
  });
};

export const FILE_FORMAT_PREVIEW_REQUEST = 'FILE_FORMAT_PREVIEW_REQUEST';
export const FILE_FORMAT_PREVIEW_SUCCESS = 'FILE_FORMAT_PREVIEW_SUCCESS';
export const FILE_FORMAT_PREVIEW_FAILURE = 'FILE_FORMAT_PREVIEW_FAILURE';

export function loadFilePreview(urlPath, values, viewId) {
  const meta = {viewId};

  const apiCall = new APIV2Call().paths(urlPath);

  return {
    [RSAA]: {
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
      endpoint: apiCall
    }
  };
}

export const RESET_FILE_FORMAT_PREVIEW = 'RESET_FILE_FORMAT_PREVIEW';
export function resetFileFormatPreview() {
  return {type: RESET_FILE_FORMAT_PREVIEW};
}


//=== Load and Save file format group of cancellable actions
// _SAVE_REQUEST can cancel _LOAD_REQUEST
// save is not cancellable and blocks the UI until it completes

const loadAndSaveGroupName = 'FILE_FORMAT_LOAD_AND_SAVE_GROUP';

export const FILE_FORMAT_LOAD_REQUEST = 'FILE_FORMAT_LOAD_REQUEST';
export const FILE_FORMAT_LOAD_SUCCESS = 'FILE_FORMAT_LOAD_SUCCESS';
export const FILE_FORMAT_LOAD_FAILURE = 'FILE_FORMAT_LOAD_FAILURE';

export function loadFileFormat(formatUrl, viewId) {
  const abortController = new AbortController(); // eslint-disable-line no-undef
  const meta = {
    viewId
  };
  const abortInfo = apiUtils.getAbortInfo(loadAndSaveGroupName);
  const nonAbortableMeta = {...meta, abortInfo};
  // if AbortController is supported by browser/polyfill, then use it in meta and add options signal to fetch
  const abortableMeta = { ...meta, abortInfo: {...abortInfo, abortController}};

  const apiCall = new APIV2Call().paths(formatUrl);

  const rsaa = {
    types: [
      {type: FILE_FORMAT_LOAD_REQUEST, meta: abortableMeta},
      schemaUtils.getSuccessActionTypeWithSchema(FILE_FORMAT_LOAD_SUCCESS, fileFormatSchema, nonAbortableMeta),
      {type: FILE_FORMAT_LOAD_FAILURE, meta: nonAbortableMeta}],
    method: 'GET',
    endpoint: apiCall
  };
  if (abortController) {
    rsaa.options = {signal: abortController.signal};
  }

  return {
    [RSAA]: rsaa
  };
}

export const FILE_FORMAT_SAVE_REQUEST = 'FILE_FORMAT_SAVE_REQUEST';
export const FILE_FORMAT_SAVE_SUCCESS = 'FILE_FORMAT_SAVE_SUCCESS';
export const FILE_FORMAT_SAVE_FAILURE = 'FILE_FORMAT_SAVE_FAILURE';

export function saveFileFormat(resourcePath, values, viewId) {
  const meta = {
    viewId,
    invalidateViewIds: ['HomeContents']
  };
  const abortInfo = apiUtils.getAbortInfo(loadAndSaveGroupName);
  const nonAbortableMeta = {...meta, abortInfo};

  const apiCall = new APIV2Call().paths(resourcePath);

  return {
    [RSAA]: {
      types: [
        {type: FILE_FORMAT_SAVE_REQUEST, meta: nonAbortableMeta},
        {type: FILE_FORMAT_SAVE_SUCCESS, meta},
        {type: FILE_FORMAT_SAVE_FAILURE, meta}
      ],
      method: 'PUT',
      body: addFileModalMapper.unescapeFilePayload(values),
      headers: {'Content-Type': 'application/json'},
      endpoint: apiCall
    }
  };
}
//=== end of Load and Save file format group

export const UPLOAD_FINISH_REQUEST = 'UPLOAD_FINISH_REQUEST';
export const UPLOAD_FINISH_SUCCESS = 'UPLOAD_FINISH_SUCCESS';
export const UPLOAD_FINISH_FAILURE = 'UPLOAD_FINISH_FAILURE';

function postUploadFinish(file, values, viewId) {
  const resourcePath = file.getIn(['links', 'upload_finish']);
  const meta = {
    viewId,
    invalidateViewIds: ['HomeContents']
  };

  const apiCall = new APIV2Call().fullpath(resourcePath);

  return {
    [RSAA]: {
      types: [
        {type: UPLOAD_FINISH_REQUEST, meta},
        {type: UPLOAD_FINISH_SUCCESS, meta},
        {type: UPLOAD_FINISH_FAILURE, meta}
      ],
      method: 'POST',
      body: addFileModalMapper.unescapeFilePayload(values),
      headers: {'Content-Type': 'application/json'},
      endpoint: apiCall
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

  const apiCall = new APIV2Call().fullpath(resourcePath);

  return {
    [RSAA]: {
      types: [
        {type: UPLOAD_CANCEL_REQUEST},
        {type: UPLOAD_CANCEL_SUCCESS},
        {type: UPLOAD_CANCEL_FAILURE}
      ],
      method: 'POST',
      body: addFileModalMapper.unescapeFilePayload(file.get('fileFormat').toJS()),
      headers: {'Content-Type': 'application/json'},
      endpoint: apiCall
    }
  };
}

export function uploadCancel(file) {
  return (dispatch) => {
    return dispatch(postUploadCancel(file));
  };
}
