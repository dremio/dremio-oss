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
import { getEntityType } from '@app/utils/pathUtils';

import { HOME_SPACE_NAME, RECENT_SPACE_NAME } from 'constants/Constants';

import { denormalizeFile } from 'selectors/resources';
import { getUserName } from 'selectors/account';

function _getResourceName(resourceName) {
  if (resourceName === 'home' || !resourceName) {
    return HOME_SPACE_NAME;
  } else if (resourceName === 'recent') {
    return RECENT_SPACE_NAME;
  }
  return resourceName;
}

function _getPageType(pageType) {
  if (pageType === 'home' || pageType === 'recent' || !pageType) {
    return 'space';
  }
  return pageType;
}

function _getDataListToShow(state, props, name) {
  const resourceName = _getResourceName(props.routeParams.resourceId);
  const pageType = _getPageType(props.pageType);
  const availibleEntities = state.resources.view[pageType];
  if (!availibleEntities.get(resourceName)) {
    return Immutable.List();
  }
  return availibleEntities.getIn([resourceName, name]);
}

function getDatasetsList(state, props) {
  const { entities } = state.resources;
  return _getDataListToShow(state, props, 'datasets').map(name => {
    const dataset = entities.getIn(['dataset', name]);
    return dataset.set('datasetConfig', entities.getIn(['datasetConfig', dataset.get('datasetConfig')]));
  }
);
}

function getFoldersList(state, props) {
  const { entities } = state.resources;
  return _getDataListToShow(state, props, 'folders').map(name => {
    return entities.getIn(['folder', name]);
  });
}

function getFileList(state, props) {
  const { entities } = state.resources;
  return _getDataListToShow(state, props, 'files').map(name => {
    return entities.getIn(['file', name]);
  });
}

function getResourceProgressState(state, props) {
  const resourceName = _getResourceName(props.routeParams.resourceId);
  const pageType = _getPageType(props.pageType);
  const availibleEntities = state.resources.view[pageType];
  const resource = availibleEntities.get(resourceName);
  if (!resource) {
    return false;
  }
  const filesSize = resource.get('files') && resource.get('files').size;
  const folderssSize = resource.get('folders') && resource.get('folders').size;
  const datasetsSize = resource.get('datasets') && resource.get('datasets').size;
  const physicalDatasetsSize = resource.get('physicalDatasets') && resource.get('physicalDatasets').size;
  return resource.get('isInProgress') && !filesSize &&
    !folderssSize && !datasetsSize && !physicalDatasetsSize;
}

// function _getRecentSpace(spaces) {
//   return (spaces.find(val => val.get('name') === RECENT_SPACE_NAME) || Immutable.Map())
//   .merge(Immutable.Map({
//     name: 'Recent',
//     arrow: false,
//     disabled: true, // TODO should be false when api appear
//     icon: 'Recent',
//     opened: false,
//     selected: false,
//     children: Immutable.List()
//   }));
// }

function _isEntityExpanded(state, entity) {
  return state.ui.getIn(['resourceTree', 'nodes', entity.get('id'), 'expanded']);
}

function createTreeNode(entity, expanded) {
  return Immutable.Map({
    id: entity.get('id'),
    entity,
    expanded
  });
}

function _getTreeChildrenFromContents(state, contents) {
  if (!contents) {
    return Immutable.Map();
  }
  const {entities} = state.resources;
  return Immutable.List().concat(
    contents.get('folders').map(key =>
      _getTreeNodeFromEntity(state, entities.getIn(['folder', key]))),
    contents.get('files').map(key =>
      createTreeNode(entities.getIn(['file', key]))),
    contents.get('datasets').map(key =>
      createTreeNode(entities.getIn(['dataset', key]))),
    contents.get('physicalDatasets').map(key =>
      createTreeNode(entities.getIn(['physicalDataset', key])))
  );
}

/**
 * Denormalize tree along a specific path
 */
function _getTreeNodeFromEntity(state, entity) {
  const expanded = _isEntityExpanded(state, entity);
  if (expanded) {
    return createTreeNode(entity, expanded)
      .set('children', _getTreeChildrenFromContents(state, entity.get('contents')));
  }
  return createTreeNode(entity);
}

const _getSummaryDataset = (state, fullPath) => {
  return state.resources.entities.getIn(['summaryDataset', fullPath]) || Immutable.Map();
};

export const getSummaryDataset = createSelector(
  [ _getSummaryDataset ],
  datasets => {
    return datasets;
  }
);

export const getDatasets = createSelector(
  [ getDatasetsList ],
  datasets => {
    return datasets;
  }
);

export const getFolders = createSelector(
  [ getFoldersList ],
  folders => {
    return folders;
  }
);

export const getFiles = createSelector(
  [ getFileList ],
  files => {
    return files;
  }
);

export const isSpaceContentInProgress = createSelector(
  [ getResourceProgressState ],
  isInProgress => {
    return isInProgress;
  }
);

export const getHomePageEntity = (state, urlPath) => {
  const { entities } = state.resources;
  const entityType = getEntityType(urlPath);
  const userName = getUserName(state);
  const finalUrlPath = urlPath === '/' ? `/home/%40${encodeURIComponent(userName)}` : urlPath;
  const entity = entities.get(entityType).find(e => e.getIn(['links', 'self']) === finalUrlPath); // todo: safe for all types?
  if (!entity) {
    return;
  }
  return entity;
};

// todo: why is this called getHomeContents - seems to do way more than "home"?
// todo: simplify this
// The only remaining places this is used are in AddFileModal, and AddFolderModal,
// which are not actually using it to get "HomeContents".
// They are just using it to get the parent entity (folder/space/source/home).
export function getHomeContents(state, urlPath) {
  const entity = getHomePageEntity(state, urlPath);

  if (!entity) {
    return;
  }

  return entity.set('contents', denormalizeHomeContents(state, entity.get('contents')));
}

function denormalizeHomeContents(state, contents) {
  if (!contents) {
    return Immutable.Map();
  }
  const {entities} = state.resources;
  return Immutable.Map({
    datasets: contents.get('datasets').map(key => entities.getIn(['dataset', key])),
    files: contents.get('files').map(key => denormalizeFile(state, key)),
    folders: contents.get('folders').map(key => entities.getIn(['folder', key])),
    physicalDatasets: contents.get('physicalDatasets').map(key => entities.getIn(['physicalDataset', key]))
  });
}
