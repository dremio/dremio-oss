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
import { collapseExploreSql } from '@app/actions/explore/ui';
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';
import { getPathPart, changePageTypeInUrl } from '@app/pages/ExplorePage/pageTypeUtils';

import { API_URL_V2 } from 'constants/Api';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import apiUtils from '@app/utils/apiUtils/apiUtils';

export const RUN_TABLE_TRANSFORM_START   = 'RUN_TABLE_TRANSFORM_START';
export const RUN_TABLE_TRANSFORM_SUCCESS = 'RUN_TABLE_TRANSFORM_SUCCESS';
export const RUN_TABLE_TRANSFORM_FAILURE = 'RUN_TABLE_TRANSFORM_FAILURE';

/**
 * common helper for different table operations
 */
export function postDatasetOperation({
  href,
  schema,
  viewId,
  dataset,
  uiPropsForEntity,
  invalidateViewIds,
  body,
  notificationMessage,
  metas = [],
  nextTable
}) {
  const meta = {
    viewId, invalidateViewIds, dataset, entity: dataset, nextTable, href
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
      headers: {
        'Content-Type': 'application/json',
        ...apiUtils.getJobDataNumbersAsStringsHeader()
      },
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

export function navigateToNextDataset(response, {
    replaceNav,
    linkName,
    isSaveAs,
    preserveTip,
    // we need to change a pathname only in the following cases
    // 1) Save as
    // 2) Edit original sql // handled by navigateAfterReapply
    // 3) When we write a new query and click Preview/Run to navigate to newUntitled page
    changePathname
  } = {}) {
  return (dispatch, getStore) => {
    changePathname = isSaveAs || changePathname;
    const location = getStore().routing.locationBeforeTransitions;
    const { tipVersion } = location.query || {};
    let targetPageType = PageTypes.default;
    let keepQuery = false;
    let collapseSqlEditor = false;

    //for graph and wiki we have to keep query parameters and redirect to corresponding page
    for (const pageType of [PageTypes.graph, PageTypes.wiki]) {
      const urlPart = getPathPart(pageType);
      //check if url ends with page type
      if (location.pathname.endsWith(urlPart)) {
        targetPageType = pageType;
        keepQuery = true;
        collapseSqlEditor = true; // collapse an editor for wiki and graph pages
        break;
      }
    }

    if (collapseSqlEditor) {
      dispatch(collapseExploreSql());
    }

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
    const pathname = changePageTypeInUrl(changePathname ? parsedLink.pathname : location.pathname, targetPageType);
    const query = {
      ...(keepQuery ? location.query : {}), // Initial dataset request will navigate. Need to not clobber graph query params.
      ...parsedLink.query,
      ...(jobId ? { jobId } : {}),
      ...(mode ? { mode } : {}),
      version: nextVersion,
      tipVersion: preserveTip ? tipVersion || nextVersion : nextVersion
    };
    const action = replaceNav ? replace : push;
    const state = isSaveAs ? { afterDatasetSave: true } : {};
    return dispatch(action({ pathname, query, state }));
  };
}
