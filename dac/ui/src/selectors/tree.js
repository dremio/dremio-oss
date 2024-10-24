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
import { createSelector } from "reselect";
import Immutable from "immutable";

const getResourceTreeData = (state) => {
  return (
    Immutable.fromJS(state.resources.entities.get("tree")) || Immutable.List()
  );
};

const getStarredItemIdsData = (state) => {
  return state.resources.stars.get("starResourceList") || Immutable.List();
};

export const getStarredItemIds = createSelector(
  [getStarredItemIdsData],
  (starredItemList) => {
    const returnVal = [];
    for (let i = 0; i < starredItemList.size; i++) {
      const newItem = {};
      newItem.id = starredItemList.getIn([i, "id"]);
      returnVal.push(newItem);
    }
    return returnVal;
  },
);

const getStarredResourcesData = (state) => {
  return state.resources.stars.get("starResourceList") || Immutable.List();
};

const getResourceTreeModalData = (state) => {
  return state.resources.entities.get("treeModal") || Immutable.List();
};

export const getStarredResources = createSelector(
  [getStarredResourcesData],
  (starredResourceList) => {
    return Immutable.List(starredResourceList);
  },
);

export const getResourceTreeModal = createSelector(
  [getResourceTreeModalData],
  (resourceModalList) => {
    return Immutable.List(resourceModalList);
  },
);

export const getResourceTree = createSelector([getResourceTreeData], (tree) =>
  tree.sortBy((t) => t.get("type") !== "HOME" && t.get("name")),
);
