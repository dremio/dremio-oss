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
import { push, replace } from 'react-router-redux';
import urlParse from 'url-parse';

import { API_URL_V2 } from 'constants/Api';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const RUN_TABLE_TRANSFORM_START   = 'RUN_TABLE_TRANSFORM_START';
export const RUN_TABLE_TRANSFORM_SUCCESS = 'RUN_TABLE_TRANSFORM_SUCCESS';
export const RUN_TABLE_TRANSFORM_FAILURE = 'RUN_TABLE_TRANSFORM_FAILURE';

export const runTableTransformActionTypes = [
  RUN_TABLE_TRANSFORM_START,
  RUN_TABLE_TRANSFORM_SUCCESS,
  RUN_TABLE_TRANSFORM_FAILURE
];

/**
 * common helper for different table operations
 */
export function postDatasetOperation({
  href, schema, viewId, dataset, uiPropsForEntity, invalidateViewIds, body, type,
  notificationMessage, metas = [], nextTable, replaceNav
}) {
  const meta = {
    viewId, invalidateViewIds, dataset, entity: dataset, nextTable, href, type, replaceNav
  };
  const successMeta = notificationMessage ? {
    ...meta,
    notification: {
      message: notificationMessage,
      level: 'success'
    }
  } : meta;
  return {
    [CALL_API]: {
      types: [
        { type: RUN_TABLE_TRANSFORM_START, meta: {...meta, ...metas[0]} },
        schemaUtils.getSuccessActionTypeWithSchema(RUN_TABLE_TRANSFORM_SUCCESS, schema,
          {...successMeta, ...metas[1]},
          uiPropsForEntity
        ),
        { type: RUN_TABLE_TRANSFORM_FAILURE, meta: {...meta, ...metas[2]} }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: body && JSON.stringify(body),
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function _getNextJobId(fullDataset) {
  const newJobId = fullDataset.getIn(['jobId', 'id']);
  // only add the jobId to the query for run results (not approximate)
  return fullDataset.get('approximate') ? undefined : newJobId;
}

export function navigateToNextDataset(response, {replaceNav, linkName, isSaveAs, preserveTip} = {}) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    const { tipVersion } = location.query || {};
    //if isGraphLink is true then means user should be redirected to dataset graph
    const isGraphLink = location.pathname.substr(-6) === '/graph';
    const payload = response.payload || Immutable.Map();
    const resultId = payload.get('result');
    const nextDataset = payload.getIn(['entities', 'datasetUI', resultId]);
    const fullDataset = payload.getIn(['entities', 'fullDataset', resultId]);
    if (!nextDataset || !fullDataset) {
      throw new Error('transform did not return next dataset');
    }

    const nextVersion = nextDataset.get('datasetVersion');

    const link = nextDataset && nextDataset.getIn(['links', linkName || 'self']) || '';
    const parsedLink = urlParse(link, true);

    const mode = isSaveAs ? 'edit' : location.query && location.query.mode;
    const jobId = _getNextJobId(fullDataset);
    const pathname = isGraphLink ? `${parsedLink.pathname}/graph` : parsedLink.pathname;
    const query = {
      ...(isGraphLink ? location.query : {}), // Initial dataset request will navigate. Need to not clobber graph query params.
      ...parsedLink.query,
      ...(jobId ? { jobId } : {}),
      ...(mode ? { mode } : {}),
      version: nextVersion,
      tipVersion: preserveTip ? tipVersion || nextVersion : nextVersion
    };
    const action = replaceNav ? replace : push;
    return dispatch(action({ pathname, query, state: {}}));
  };
}
