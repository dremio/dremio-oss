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
import { createSelector } from 'reselect';
import Immutable from 'immutable';

import { constructFullPathAndEncode } from 'utils/pathUtils';

import { CREATED_SOURCE_NAME } from 'reducers/resources/sourceList';
import { EXPLORE_VIEW_ID } from 'reducers/explore/view';

// todo: reevaluate viewId system:
// what happens if the same call is made twice before the previous has returned (e.g. type-ahead search)
// is difficult to use if you need to make multiple requests in sequence or parallel
// makes many assumptions that a view is only ever used once at a time
// feels an aweful lot like reinventing Promises
const BLANK_VIEW_STATES = {};
export function getViewState(state, viewId) {
  return (state.resources && state.resources.view.get(viewId))
    || BLANK_VIEW_STATES[viewId]
    || (BLANK_VIEW_STATES[viewId] = Immutable.Map({viewId})); // cache so that purerender will not see a change
}

export function getExploreViewState(state) {
  const viewId = EXPLORE_VIEW_ID;
  return getViewState(state, viewId);
}

function getSearchData(state) {
  return state.search.get('searchDatasets');
}

function _getCreatedSource(state) {
  return state.resources.sourceList.get(CREATED_SOURCE_NAME);
}

export const getCreatedSource = createSelector(
  [ _getCreatedSource ],
  source => {
    return source;
  }
);

export const getSearchResult = createSelector([ getSearchData ], datasets => datasets);

export function getEntity(state, entityId, entityType) {
  return state.resources.entities.getIn([entityType, entityId]);
}

export function getFolder(state, folderId) {
  return state.resources.entities.getIn(['folder', folderId]);
}

export const getDescendantsList = (state) => {
  return state.resources.entities.getIn(['datasetUI', 'descendantsList']);
};

export const getParentList = (state) => state.resources.entities.getIn(['datasetUI', 'parentList']);

export const getDatasetAcceleration = (state, fullPath) => {
  const constructedFullPath = constructFullPathAndEncode(fullPath);
  const allAccelerations = state.resources.entities.get('datasetAcceleration', Immutable.Map());
  return allAccelerations.find((item) => {
    return item.get('fullPath') === constructedFullPath;
  });
};

export const searchEntity = (state, entityType, findBy, value) => {
  let entity;
  state.resources.entities.get(entityType).forEach(element => {
    if (element.get(findBy) === value) {
      entity = element;
    }
  });
  return entity;
};
