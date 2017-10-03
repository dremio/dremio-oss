/*
 * Copyright (C) 2017 Dremio Corporation
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

import { getEntityType, getRootEntityType, constructFullPathAndEncode } from 'utils/pathUtils';
import {humanSorter} from 'utils/sort';

import { datasetTypeToEntityType } from 'constants/datasetTypes';

import { getUserName } from 'selectors/account';

import { CREATED_SOURCE_NAME } from 'reducers/resources/sourceList';

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

export function getSpaces(state) {
  const { entities, spaceList } = state.resources;
  return spaceList.get('spaces').map(spaceId => {
    const spaceName = entities.getIn(['space', spaceId, 'name']);
    return entities.getIn(['space', spaceId]).merge(spaceList.getIn(['spacesById', spaceName]));
  });
}

export function getSources(state) {
  const { entities, sourceList } = state.resources;
  return sourceList.get('sources').map(sourceId => {
    const sourceName = entities.getIn(['source', sourceId, 'name']);
    return entities.getIn(['source', sourceId]).merge(sourceList.getIn(['sourcesById', sourceName]));
  });
}

export function getErrorFromRootEntity(state, urlPath) {
  const rootEntity = getRootEntityByUrlPath(state, urlPath);
  return rootEntity && rootEntity.getIn(['state', 'messages', 0, 'message']);
}

function getSearchData(state) {
  return state.search.get('searchDatasets');
}

function getSortedResource(resources) {
  return resources.sort((a, b) => {
    const ret = Number(b.get('isActivePin') || 0) - Number(a.get('isActivePin') || 0);
    return ret !== 0 ? ret : humanSorter(a.get('name'), b.get('name'));
  }).toList();
}

function _getCreatedSource(state) {
  return state.resources.sourceList.get(CREATED_SOURCE_NAME);
}

export const getSortedSpaces = createSelector(
  [ getSpaces ],
  spaces => {
    return getSortedResource(spaces);
  }
);

export const getSortedSources = createSelector(
  [ getSources ],
  sources => {
    return getSortedResource(sources);
  }
);

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

// todo: This is only used to get source level errors in browse page.
// Consider having browse request return source level errors instead of having to get it in separate all sources request.
export function getRootEntityByUrlPath(state, urlPath) {
  const entityType = getRootEntityType(urlPath);

  let subPath;
  if (entityType === 'home') { // todo: simplify this
    subPath = `/home/@${getUserName(state)}`;
  } else {
    subPath = urlPath.split('/').slice(0, 3).join('/');
  }
  return state.resources.entities.get(entityType).find(e => e.get('resourcePath') === subPath);
}

export function getEntityByUrlPath(state, urlPath, entityType) {
  const finalEntityType = entityType || getEntityType(urlPath);
  const entities = state.resources.entities.get(finalEntityType);
  const result = entities.findEntry((x) => x.getIn(['links', 'self']) === urlPath);
  if (result) {
    return result[1];
  }
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

// for explore page which unfortunately has url for 'dataset' but not id
export function findDatasetForDatasetType(state, datasetType, datasetUrl) {
  const entityType = datasetTypeToEntityType[datasetType];
  if (!entityType) {
    throw Error('unknown datasetType ' + datasetType);
  }
  return state.resources.entities.get(entityType).find(e => e.getIn(['links', 'self']) === datasetUrl);
}

export const getDatasetAcceleration = (state, fullPath) => {
  const constructedFullPath = constructFullPathAndEncode(fullPath);
  const allAccelerations = state.resources.entities.get('datasetAcceleration', Immutable.Map());
  return allAccelerations.find((item) => {
    return item.get('fullPath') === constructedFullPath;
  });
};
