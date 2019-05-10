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
import { createSelector } from 'reselect';
import Immutable from 'immutable';

import { constructFullPathAndEncode } from 'utils/pathUtils';

import { CREATED_SOURCE_NAME } from 'reducers/resources/sourceList';
import { EXPLORE_VIEW_ID } from 'reducers/explore/view';
import { getSortedResourceSelector, isEntityPinned, addPinStateToList } from '@app/selectors/home';
import { ENTITY_TYPES } from '@app/constants/Constants';

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

function getSpacesImpl(state) {
  const { entities } = state.resources;
  return entities.get(ENTITY_TYPES.space);
}

export const getSpaces = addPinStateToList(createSelector(getSpacesImpl, spaceMap => spaceMap.toList()));

export function getSources(state) {
  const { entities, sourceList } = state.resources;
  return sourceList.get('sources').map(sourceId => {
    return entities.getIn(['source', sourceId]).merge({
      isActivePin: isEntityPinned(state, sourceId)
    });
  });
}

function getSearchData(state) {
  return state.search.get('searchDatasets');
}


function _getCreatedSource(state) {
  return state.resources.sourceList.get(CREATED_SOURCE_NAME);
}

export const getSortedSpaces = getSortedResourceSelector(getSpaces);
export const getSortedSources = getSortedResourceSelector(getSources);

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

export function denormalizeFile(state, fileId) {
  const {entities} = state.resources;
  const file = entities.getIn(['file', fileId]);
  if (file) {
    return file.get('fileFormat') ?
      file.set('fileFormat', entities.getIn(['fileFormat', file.get('fileFormat')]))
      : file;
  }
}

export const getDatasetAcceleration = (state, fullPath) => {
  const constructedFullPath = constructFullPathAndEncode(fullPath);
  const allAccelerations = state.resources.entities.get('datasetAcceleration', Immutable.Map());
  return allAccelerations.find((item) => {
    return item.get('fullPath') === constructedFullPath;
  });
};

/**
 * Evaluates entity type from redux store, by checking whether id is presented in space, source list
 * and return 'home' as a type if entity is not a space or source
 * @param {string} entityId
 * @returns {ENTITY_TYPES} - on of the following types: home, space, source
 */
export const getRootEntityTypeById = (state, entityId) => {
  if (entityId) {
    const typesToCheck = [ENTITY_TYPES.source, ENTITY_TYPES.space];
    for (const type of typesToCheck) {
      if (getEntity(state, entityId, type)) {
        return type;
      }
    }
  }
  return ENTITY_TYPES.home; // is it fair
};

/**
 * works for sources, spaces and home space
 * @param {object} state
 * @param {string} entityId
 * @returns {Immutable.List<string>} a path ofr the entity
 */
export const getEntityPath = (state, entityId) => {
  const type = getRootEntityTypeById(state, entityId);
  const entity = getEntity(state, entityId, type);
  return type === ENTITY_TYPES.home ? new Immutable.List([]) : entity.get('fullPathList');
};

/**
 * Get a link url for space/source/home space
 *
 * @param {object} state
 * @param {string} entityId
 * @returns {string} a browser url to folder/space/home space
 */
export const getEntityLinkUrl = (state, entityId) => {
  const type = getRootEntityTypeById(state, entityId);
  const path = getEntityPath(state, entityId);

  return '/' + path.insert(0, type).map(encodeURIComponent).join('/');
};
