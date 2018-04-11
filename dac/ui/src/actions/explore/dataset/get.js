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

export const LOAD_EXPLORE_ENTITIES_STARTED = 'LOAD_EXPLORE_ENTITIES_STARTED';
export const LOAD_EXPLORE_ENTITIES_SUCCESS = 'LOAD_EXPLORE_ENTITIES_SUCCESS';
export const LOAD_EXPLORE_ENTITIES_FAILURE = 'LOAD_EXPLORE_ENTITIES_FAILURE';

function fetchEntities({ href, schema, viewId, uiPropsForEntity, invalidateViewIds }) {
  const meta = { viewId, invalidateViewIds, href };
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_EXPLORE_ENTITIES_STARTED, meta },
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_EXPLORE_ENTITIES_SUCCESS, schema, meta, uiPropsForEntity),
        { type: LOAD_EXPLORE_ENTITIES_FAILURE, meta }
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export const loadExploreEntities = ({ href, schema, viewId, uiPropsForEntity, invalidateViewIds }) =>
  (dispatch) => dispatch(
    fetchEntities({ href, schema, viewId, uiPropsForEntity, invalidateViewIds })
  );

export const PERFORM_LOAD_DATASET = 'PERFORM_LOAD_DATASET';

export const performLoadDataset = (dataset, viewId, callback) => {
  return {type: PERFORM_LOAD_DATASET, meta: {dataset, viewId, callback}};
};

export const CLEAN_DATA_VIEW_ID = 'CLEAN_DATA_VIEW_ID';

export const LOAD_CLEAN_DATA_START   = 'LOAD_CLEAN_DATA_START';
export const LOAD_CLEAN_DATA_SUCCESS = 'LOAD_CLEAN_DATA_SUCCESS';
export const LOAD_CLEAN_DATA_FAILURE = 'LOAD_CLEAN_DATA_FAILURE';

export function loadCleanData(colName, dataset) {
  return (dispatch) => {
    return dispatch(loadCleanDataFetch(colName, dataset));
  };
}

function loadCleanDataFetch(colName, dataset) {
  const data = { colName };
  const meta = { viewId: CLEAN_DATA_VIEW_ID };

  return {
    [CALL_API]: {
      types: [
        { type: LOAD_CLEAN_DATA_START, meta },
        { type: LOAD_CLEAN_DATA_SUCCESS, meta },
        { type: LOAD_CLEAN_DATA_FAILURE, meta }
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
      endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/clean`
    }
  };
}


