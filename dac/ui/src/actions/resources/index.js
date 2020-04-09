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

import schemaUtils from 'utils/apiUtils/schemaUtils';
import * as schemas from 'schemas';
import { datasetTypeToEntityType } from '@app/constants/datasetTypes';
import { APIV2Call } from '@app/core/APICall';

export const LOAD_ENTITIES_STARTED = 'LOAD_ENTITIES_STARTED';
export const LOAD_ENTITIES_SUCCESS = 'LOAD_ENTITIES_SUCCESS';
export const LOAD_ENTITIES_FAILURE = 'LOAD_ENTITIES_FAILURE';

function fetchEntities(urlPath, schema, viewId) {
  const resourcePath = urlPath;
  const meta = { resourcePath, viewId };

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .uncachable();

  return {
    [RSAA]: {
      types: [
        { type: LOAD_ENTITIES_STARTED, meta},
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_ENTITIES_SUCCESS, schema, meta),
        { type: LOAD_ENTITIES_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadEntities(urlPath, schema, viewId) {
  return (dispatch) => {
    return dispatch(fetchEntities(urlPath, schema, viewId));
  };
}

export function loadDatasetForDatasetType(datasetType, datasetUrl, viewId) {
  const schema = schemas[datasetTypeToEntityType[datasetType]];
  if (!schema) {
    throw new Error('unknown datasetType ' + datasetType);
  }
  return loadEntities(datasetUrl, schema, viewId);
}

export const RENAME_ENTITY_STARTED = 'RENAME_ENTITY_STARTED';
export const RENAME_ENTITY_SUCCESS = 'RENAME_ENTITY_SUCCESS';
export const RENAME_ENTITY_FAILURE = 'RENAME_ENTITY_FAILURE';

function postRenameHomeEntity(entity, entityType, newName, invalidateViewIds) {
  const resourcePath = entity.getIn(['links', 'rename']);
  const schema = schemas[entityType];
  const meta = { invalidateViewIds };

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .params({renameTo: newName});

  return {
    [RSAA]: {
      types: [
        { type: RENAME_ENTITY_STARTED, meta},
        schemaUtils.getSuccessActionTypeWithSchema(RENAME_ENTITY_SUCCESS, schema, meta),
        { type: RENAME_ENTITY_FAILURE, meta}
      ],
      method: 'POST',
      endpoint: apiCall
    }
  };
}

export function renameHomeEntity(entity, entityType, newName, invalidateViewIds) {
  return (dispatch) => {
    return dispatch(postRenameHomeEntity(entity, entityType, newName, invalidateViewIds));
  };
}

export const RESET_VIEW_STATE = 'RESET_VIEW_STATE';

export function resetViewState(viewId) {
  return {
    type: RESET_VIEW_STATE, meta: {viewId}
  };
}

export const UPDATE_VIEW_STATE = 'UPDATE_VIEW_STATE';

export function updateViewState(viewId, viewState) {
  return {
    type: UPDATE_VIEW_STATE, meta: {viewId, viewState}
  };
}

export const DISMISS_VIEW_STATE_ERROR = 'DISMISS_VIEW_STATE_ERROR';
export const dismissViewStateError = (viewId) => ({ type: DISMISS_VIEW_STATE_ERROR, meta: { viewId } });
