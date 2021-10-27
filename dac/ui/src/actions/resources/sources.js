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
import { arrayOf } from 'normalizr';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import actionUtils from 'utils/actionUtils/actionUtils';
import sourceSchema from 'dyn-load/schemas/source';
import {updatePrivileges} from 'dyn-load/actions/resources/sourcesMixin';

import sourcesMapper from 'utils/mappers/sourcesMapper';
import { getUniqueName } from 'utils/pathUtils';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import { getSourceNames, getSpaceNames } from '@app/selectors/home';
import { APIV2Call } from '@app/core/APICall';

export const ADD_NEW_SOURCE_START = 'ADD_NEW_SOURCE_START';
export const ADD_NEW_SOURCE_SUCCESS = 'ADD_NEW_SOURCE_SUCCESS';
export const ADD_NEW_SOURCE_FAILURE = 'ADD_NEW_SOURCE_FAILURE';

function postCreateSource(sourceModel, meta, _shouldShowFailureNotification = false) {
  meta = {
    ...meta,
    source: Immutable.fromJS(sourceModel).merge({resourcePath: `/source/${sourceModel.name}`}),
    invalidateViewIds: ['AllSources']
  };
  const failureMeta = _shouldShowFailureNotification ? { ...meta, notification: true } : meta;

  const apiCall = new APIV2Call()
    .paths(`source/${encodeURIComponent(sourceModel.name)}`)
    .uncachable();

  return {
    [RSAA]: {
      types: [
        { type: ADD_NEW_SOURCE_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(ADD_NEW_SOURCE_SUCCESS, sourceSchema, meta),
        { type: ADD_NEW_SOURCE_FAILURE, meta: failureMeta}
      ],
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(sourceModel),
      endpoint: apiCall
    }
  };
}

export function createSource(data, sourceType) {
  const sourceModel = sourcesMapper.newSource(sourceType, data);
  const grantLength = data.accessControlList ? data.accessControlList.grants.length : 0;
  const userControls = (grantLength > 0 ? data.accessControlList.grants.filter(g => g.granteeType.toLowerCase() === 'user') : []).map(u => ({
    id: u.id,
    permissions: u.privileges
  }));
  const roleControls = (grantLength > 0 ? data.accessControlList.grants.filter(g => g.granteeType.toLowerCase() === 'role') : []).map(r => ({
    id: r.id,
    permissions: r.privileges
  }));
  const finalSourceModel = {
    ...sourceModel,
    accessControlList: {
      userControls,
      roleControls
    }
  };
  return (dispatch) => {
    return dispatch(postCreateSource(finalSourceModel));
  };
}

export function updateSourcePrivileges(data, sourceType) {
  return updatePrivileges(data, sourceType);
}

export function createSampleSource(meta) {
  return (dispatch, getState) => {
    const state = getState();
    const sources = getSourceNames(state);
    const spaces = getSpaceNames(state);
    const existingNames = new Set([...sources, ...spaces]);

    const name = getUniqueName(la('Samples'), (nameTry) => {
      return !existingNames.has(nameTry);
    });

    const sourceModel = {
      'config': {
        'externalBucketList': [
          'samples.dremio.com'
        ],
        'credentialType': 'NONE',
        'secure': false,
        'propertyList': []
      },
      name,
      'accelerationRefreshPeriod': DataFreshnessSection.defaultFormValueRefreshInterval(),
      'accelerationGracePeriod': DataFreshnessSection.defaultFormValueGracePeriod(),
      'accelerationNeverRefresh': true,
      'type': 'S3'
    };
    return dispatch(postCreateSource(sourceModel, meta, true));
  };
}
export function isSampleSource(source) {
  if (!source) return false;
  if (source.type !== 'S3') return false;
  if (!source.config.externalBucketList || source.config.externalBucketList.length > 1) return false;
  if (source.config.externalBucketList[0] !== 'samples.dremio.com') return false;
  return true;
}


export const SOURCES_LIST_LOAD_START = 'SOURCES_LIST_LOAD_START';
export const SOURCES_LIST_LOAD_SUCCESS = 'SOURCES_LIST_LOAD_SUCCESS';
export const SOURCES_LIST_LOAD_FAILURE = 'SOURCES_LIST_LOAD_FAILURE';

function fetchSourceListData(includeDatasetCount = false) {
  const meta = {viewId: 'AllSources', mergeEntities: true};

  const apiCall = new APIV2Call()
    .path('sources')
    .params({includeDatasetCount})
    .uncachable();

  return {
    [RSAA]: {
      types: [
        {type: SOURCES_LIST_LOAD_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(SOURCES_LIST_LOAD_SUCCESS, { sources: arrayOf(sourceSchema) }, meta),
        {type: SOURCES_LIST_LOAD_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadSourceListData() {
  return (dispatch) => {
    return dispatch(fetchSourceListData())
      .then(dispatch(fetchSourceListData(true)));
  };
}

export const REMOVE_SOURCE_START = 'REMOVE_SOURCE_START';
export const REMOVE_SOURCE_SUCCESS = 'REMOVE_SOURCE_SUCCESS';
export const REMOVE_SOURCE_FAILURE = 'REMOVE_SOURCE_FAILURE';

function fetchRemoveSource(source) {
  const name = source.get('name');
  const meta = {
    id: source.get('id'),
    name,
    invalidateViewIds: ['AllSources']
  };

  const entityRemovePaths = [['source', source.get('id')]];

  const errorMessage = la('There was an error removing the source.');

  const apiCall = new APIV2Call()
    .paths(source.getIn(['links', 'self']))
    .params({version: source.get('tag')})
    .uncachable();

  return {
    [RSAA]: {
      types: [
        {
          type: REMOVE_SOURCE_START, meta
        },
        {
          type: REMOVE_SOURCE_SUCCESS,
          meta: {...meta, success: true, entityRemovePaths, emptyEntityCache: name}
        },
        {
          type: REMOVE_SOURCE_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage)
          }
        }
      ],
      method: 'DELETE',
      endpoint: apiCall
    }
  };
}

export function removeSource(source) {
  return (dispatch) => {
    return dispatch(fetchRemoveSource(source));
  };
}

export const RENAME_SOURCE_START = 'RENAME_SOURCE_START';
export const RENAME_SOURCE_SUCCESS = 'RENAME_SOURCE_SUCCESS';
export const RENAME_SOURCE_FAILURE = 'RENAME_SOURCE_FAILURE';

function fetchRenameSource(oldName, newName) {
  const apiCall = new APIV2Call()
    .paths(`source/${oldName}/rename`)
    .params({renameTo: newName});

  return {
    [RSAA]: {
      types: [
        {
          type: RENAME_SOURCE_START,
          meta: { oldName, newName }
        },
        {
          type: RENAME_SOURCE_SUCCESS,
          meta: { oldName }
        },
        {
          type: RENAME_SOURCE_FAILURE,
          meta: { oldName }
        }
      ],
      method: 'POST',
      endpoint: apiCall
    }
  };
}

export function renameSource(oldName, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameSource(oldName, newName)).then(() => {
      return dispatch(fetchSourceListData());
    });
  };
}

export const GET_CREATED_SOURCE_START = 'GET_CREATED_SOURCE_START';
export const GET_CREATED_SOURCE_SUCCESS = 'GET_CREATED_SOURCE_SUCCESS';
export const GET_CREATED_SOURCE_FAILURE = 'GET_CREATED_SOURCE_FAILURE';

export function loadSource(sourceName, viewId) {
  const meta = {viewId};

  const apiCall = new APIV2Call()
    .paths(`source/${sourceName}`)
    .params({includeContents: false});

  return {
    [RSAA]: {
      types: [
        {type: GET_CREATED_SOURCE_START, meta},
        {type: GET_CREATED_SOURCE_SUCCESS, meta},
        {type: GET_CREATED_SOURCE_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

//http://localhost:3005/apiv2/%2Fsource%2FSamples%20(3)/?version=rVXBksYxreA%3D&nocache=1584400214778 404 (Not Found)
