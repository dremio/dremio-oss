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
import exploreUtils from 'utils/explore/exploreUtils';
import { datasetWithoutData } from 'schemas/v2/fullDataset';

export const NEW_UNTITLED_START   = 'NEW_UNTITLED_START';
export const NEW_UNTITLED_SUCCESS = 'NEW_UNTITLED_SUCCESS';
export const NEW_UNTITLED_FAILURE = 'NEW_UNTITLED_FAILURE';

export const newUntitledActionTypes = [NEW_UNTITLED_START, NEW_UNTITLED_SUCCESS, NEW_UNTITLED_FAILURE];

function newUntitledFetch(dataset, parentFullPath, viewId) {
  // todo: DX-6630: why is this called multiple times per PERFORM_NEW_UNTITLED?
  // (only one seems to be sent though)
  const meta = { viewId, entity: dataset };
  const newVersion = exploreUtils.getNewDatasetVersion();
  const href = exploreUtils.getHrefForUntitledDatasetConfig(parentFullPath, newVersion, true);
  return {
    [CALL_API]: {
      types: [
        { type: NEW_UNTITLED_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(NEW_UNTITLED_SUCCESS, datasetWithoutData, meta),
        { type: NEW_UNTITLED_FAILURE, meta }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export const newUntitled = (dataset, parentFullPath, viewId) => {
  return dispatch => dispatch(newUntitledFetch(dataset, parentFullPath, viewId));
};

export const PERFORM_NEW_UNTITLED = 'PERFORM_NEW_UNTITLED';

export const performNewUntitled = (dataset, viewId) => {
  return (dispatch) => dispatch({type: PERFORM_NEW_UNTITLED, meta: {dataset, viewId}});
};


export const NEW_UNTITLED_SQL_START = 'NEW_UNTITLED_SQL_START';
export const NEW_UNTITLED_SQL_SUCCESS = 'NEW_UNTITLED_SQL_SUCCESS';
export const NEW_UNTITLED_SQL_FAILURE = 'NEW_UNTITLED_SQL_FAILURE';

export const newUntitledSqlActionTypes = [
  NEW_UNTITLED_SQL_START, NEW_UNTITLED_SQL_SUCCESS, NEW_UNTITLED_SQL_FAILURE
];

/**
 * common helper for different table operations
 */
export function postNewUntitledSql(href, sql, queryContext, viewId) {
  const meta = { viewId };

  const body = {
    context: queryContext,
    sql
  };
  return {
    [CALL_API]: {
      types: [
        { type: NEW_UNTITLED_SQL_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(NEW_UNTITLED_SQL_SUCCESS, datasetWithoutData, meta),
        { type: NEW_UNTITLED_SQL_FAILURE, meta }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function newUntitledSql(sql, queryContext, viewId) {
  return (dispatch) => {
    const newVersion = exploreUtils.getNewDatasetVersion();
    const href = exploreUtils.getUntitledSqlHref({newVersion});
    return dispatch(postNewUntitledSql(href, sql, queryContext, viewId));
  };
}

export function newUntitledSqlAndRun(sql, queryContext, viewId) {
  return (dispatch) => {
    const newVersion = exploreUtils.getNewDatasetVersion();
    const href = exploreUtils.getUntitledSqlAndRunHref({newVersion});
    return dispatch(postNewUntitledSql(href, sql, queryContext, viewId));
  };
}
