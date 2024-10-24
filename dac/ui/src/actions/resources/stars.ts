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
import { APIV3Call } from "#oss/core/APICall";

export const LOAD_STARRED_RESOURCE_LIST_START =
  "LOAD_STARRED_RESOURCE_LIST_START";
export const LOAD_STARRED_RESOURCE_LIST_SUCCESS =
  "LOAD_STARRED_RESOURCE_LIST_SUCCESS";
export const LOAD_STARRED_RESOURCE_LIST_FAILURE =
  "LOAD_STARRED_RESOURCE_LIST_FAILURE";

const fetchStarredResource = () => {
  const meta = { viewId: "StarredItems" };
  const apiCall = new APIV3Call();

  apiCall
    .path("users")
    .path("preferences")
    .path("STARRED")
    .param("showCatalogInfo", true);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_STARRED_RESOURCE_LIST_START, meta },
        { type: LOAD_STARRED_RESOURCE_LIST_SUCCESS, meta },
        {
          type: LOAD_STARRED_RESOURCE_LIST_FAILURE,
          meta: { ...meta, notification: true },
        },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
};

export const loadStarredResources = () => (dispatch: any) =>
  dispatch(fetchStarredResource());

export const STAR_ITEM_START = "STAR_ITEM_START";
export const STAR_ITEM_SUCCESS = "STAR_ITEM_SUCCESS";
export const STAR_ITEM_FAILURE = "STAR_ITEM_FAILURE";

export const starItem = (id: string) => {
  const meta = { viewId: "StarredItems" };

  const apiCall = new APIV3Call()
    .path("users")
    .path("preferences")
    .path("STARRED")
    .path(id);

  return {
    [RSAA]: {
      types: [
        { type: STAR_ITEM_START, meta },
        { type: STAR_ITEM_SUCCESS, meta },
        { type: STAR_ITEM_FAILURE, meta: { ...meta, notification: true } },
      ],
      method: "PUT",
      endpoint: apiCall,
    },
  };
};

export const UNSTAR_ITEM_START = "UNSTAR_ITEM_START";
export const UNSTAR_ITEM_SUCCESS = "UNSTAR_ITEM_SUCCESS";
export const UNSTAR_ITEM_FAILURE = "UNSTAR_ITEM_FAILURE";

export const unstarItem = (id: string) => {
  const meta = { viewId: "StarredItems" };

  const apiCall = new APIV3Call()
    .path("users")
    .path("preferences")
    .path("STARRED")
    .path(id);

  return {
    [RSAA]: {
      types: [
        { type: UNSTAR_ITEM_START, meta },
        { type: UNSTAR_ITEM_SUCCESS, meta },
        { type: UNSTAR_ITEM_FAILURE, meta },
      ],
      method: "DELETE",
      endpoint: apiCall,
    },
  };
};
