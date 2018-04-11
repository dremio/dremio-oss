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

export const LOAD_RESOURCE_TREE_START = 'LOAD_RESOURCE_TREE_START';
export const LOAD_RESOURCE_TREE_SUCCESS = 'LOAD_RESOURCE_TREE_SUCCESS';
export const LOAD_RESOURCE_TREE_FAILURE = 'LOAD_RESOURCE_TREE_FAILURE';

const fetchResourceTree = (fullPath, { showDatasets, showSpaces, showSources, showHomes, isExpand }) => {
  const meta = { viewId: 'ResourceTree', path: fullPath, isExpand};
  const datasetsQuery = `showDatasets=${showDatasets}`;
  const query = `?${datasetsQuery}&showSources=${showSources}&showSpaces=${showSpaces}&showHomes=${showHomes}`;
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_RESOURCE_TREE_START, meta},
        { type: LOAD_RESOURCE_TREE_SUCCESS, meta},
        { type: LOAD_RESOURCE_TREE_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/resourcetree/${encodeURIComponent(fullPath)}${isExpand ? '/expand' : ''}${query}`
    }
  };
};

export const loadResourceTree = (fullPath, params) => (dispatch) => dispatch(fetchResourceTree(fullPath, params));
