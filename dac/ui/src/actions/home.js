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
import ApiUtils from 'utils/apiUtils/apiUtils';

import folderSchema from 'schemas/folder';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import actionUtils from 'utils/actionUtils/actionUtils';
import { sidebarMinWidth } from '@app/pages/HomePage/components/Columns.less';

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
  // DX-8102: invalidating home view id so that # of jobs of the folder updates
  const meta = {viewId, folderId: entity.get('id'), invalidateViewIds: ['HomeContents']};
  const successMeta = {...meta, success: true}; // doesn't invalidateViewIds without `success: true`
  const errorMessage = la('There was an error removing the format for the folder.');
  return {
    [CALL_API]: {
      types: [
        {type: CONVERT_DATASET_TO_FOLDER_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(CONVERT_DATASET_TO_FOLDER_SUCCESS, folderSchema, successMeta),
        {
          type: CONVERT_DATASET_TO_FOLDER_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
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

export const wikiActions = actionUtils.generateRequestActions('WIKI');

const wikiSuccess = (dispatch, resolvePromise, wikiData, actionDetails) => {
  const data = {
    //default values
    text: '',
    version: null,
    //--------------
    ...wikiData
  };
  dispatch({
    type: wikiActions.success,
    ...data,
    ...actionDetails
  });
  resolvePromise(data);
};



export const loadWiki = (dispatch) => entityId => {
  if (!entityId) return;
  const commonActionProps = { entityId };
  dispatch({
    type: wikiActions.start,
    ...commonActionProps
  });

  return new Promise((resolve, reject) => {
    ApiUtils.fetch(`catalog/${entityId}/collaboration/wiki`)
    .then(response => response.json().then((wikiData) => {
      wikiSuccess(dispatch, resolve, wikiData, commonActionProps);
    }), async (response) => {
      // no error message needed on 404 when wiki is not present for given id
      if (response.status === 404) {
        wikiSuccess(dispatch, resolve, {}, commonActionProps);
        return;
      }
      const errorInfo = {
        errorMessage: await ApiUtils.getErrorMessage(la('Wiki API returned an error'), response),
        errorId: '' + Math.random()
      };
      dispatch({
        type: wikiActions.failure,
        ...errorInfo,
        ...commonActionProps
      });
      reject(errorInfo);
    });
  });
};

export const WIKI_SAVED = 'WIKI_SAVED';
export const wikiSaved = (entityId, text, version) => ({
  type: WIKI_SAVED,
  entityId,
  text,
  version
});

export const MIN_SIDEBAR_WIDTH = parseInt(sidebarMinWidth, 10);
export const SET_SIDEBAR_SIZE = 'SET_SIDEBAR_SIZE';
export const setSidebarSize = size => ({
  type: SET_SIDEBAR_SIZE,
  size: Math.max(MIN_SIDEBAR_WIDTH, size)
});
