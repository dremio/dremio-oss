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
import { RSAA } from "redux-api-middleware";
import { getRefQueryParamsFromPath } from "@app/utils/nessieUtils";
import { store } from "@app/store/store";
import { APIV2Call } from "@app/core/APICall";

export const LOAD_RESOURCE_TREE_START = "LOAD_RESOURCE_TREE_START";
export const LOAD_RESOURCE_TREE_SUCCESS = "LOAD_RESOURCE_TREE_SUCCESS";
export const LOAD_RESOURCE_TREE_FAILURE = "LOAD_RESOURCE_TREE_FAILURE";

const fetchResourceTree = (
  storageName,
  viewId,
  fullPath,
  { showDatasets, showSpaces, showSources, showHomes, isExpand },
  nodeExpanded,
  currNode
) => {
  const meta = { viewId, path: fullPath, isExpand, nodeExpanded, currNode };

  const apiCall = new APIV2Call().path("resourcetree").paths(fullPath);

  if (isExpand) {
    apiCall.path("expand");
  }

  apiCall.params({
    ...getRefQueryParamsFromPath(fullPath, store.getState().nessie),
    showDatasets,
    showSources,
    showSpaces,
    showHomes,
  });

  return {
    [RSAA]: {
      types: [
        {
          type: `${
            storageName ? `${storageName}_START}` : LOAD_RESOURCE_TREE_START
          }`,
          meta,
        },
        {
          type: `${
            storageName ? `${storageName}_SUCCESS` : LOAD_RESOURCE_TREE_SUCCESS
          }`,
          meta,
        },
        {
          type: `${
            storageName ? `${storageName}_FAILURE` : LOAD_RESOURCE_TREE_FAILURE
          }`,
          meta,
        },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
};

export const loadResourceTree =
  (storageName, viewId, fullPath, params, nodeExpanded, currNode) =>
  (dispatch) =>
    dispatch(
      fetchResourceTree(
        storageName,
        viewId,
        fullPath,
        params,
        nodeExpanded,
        currNode
      )
    );
