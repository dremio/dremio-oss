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
import invariant from "invariant";
import { getUserName } from "selectors/account";
import * as fromWiki from "#oss/reducers/home/wiki";
import { isPinned } from "#oss/reducers/home/pinnedEntities";
import { humanSorter } from "utils/sort";
import * as fromContent from "#oss/reducers/home/content";
import { getLocation } from "#oss/selectors/routing";
import {
  getEntityType as getEntityTypeFromPath,
  constructFullPath,
} from "#oss/utils/pathUtils";
import { ENTITY_TYPES } from "#oss/constants/Constants";
import { generateEnumFromList } from "#oss/utils/enumUtils";
import { getEntity } from "#oss/selectors/resources";

import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import {
  addProjectBase,
  rmProjectBase,
} from "dremio-ui-common/utilities/projectBase.js";
import Immutable from "immutable";

const homeSelector = (state) => state.home;

export function getHomeForCurrentUser(state) {
  const userName = getUserName(state);

  const { entities } = state.resources;

  const entry =
    entities.get("home") &&
    entities.get("home").findEntry((home) => home.get("owner") === userName);
  if (entry) {
    return entry[1];
  }
  return Immutable.fromJS({
    id: '/home/"@' + userName + '"',
    fullPathList: ["@" + userName],
    links: {
      self: '/home/"@' + userName + '"',
    },
  });
}

export const getNormalizedEntityPath = (state) =>
  getNormalizedEntityPathByUrl(
    rmProjectBase(getLocation(state).pathname) || "/",
    getUserName(state),
  );

export const getNormalizedEntityPathByUrl = (url, currentUserName) => {
  let urlPath = url;
  if (urlPath === "/") {
    urlPath = `/home/${encodeURIComponent("@" + currentUserName)}`;
  }
  return urlPath;
};

//could be space, source, home space or folder
export const getEntityType = (state) =>
  getEntityTypeFromPath(getNormalizedEntityPath(state));

//#region wiki selectors

const wikiSelector = (state) => homeSelector(state).wiki;
export const isWikiPresent = (state) =>
  !!fromWiki.getWiki(
    wikiSelector(state),
    fromContent.getEntityId(contentSelector(state)),
  );
export const isWikiLoaded = (state, entityId) =>
  fromWiki.isWikiLoaded(wikiSelector(state), entityId);
export const getWikiValue = (state, entityId) =>
  fromWiki.getWiki(wikiSelector(state), entityId);
export const getWikiVersion = (state, entityId) =>
  fromWiki.getWikiVersion(wikiSelector(state), entityId);
export const isWikiLoading = (state, entityId) =>
  fromWiki.isWikiLoading(wikiSelector(state), entityId);
export const getErrorInfo = (state, entityId) =>
  fromWiki.getErrorInfo(wikiSelector(state), entityId);
export const getSidebarSize = (state) => homeSelector(state).sidebarSize;
export const getPinnedEntitiesState = (state) =>
  homeSelector(state).pinnedEntities;
export const isEntityPinned = (state, entityId) =>
  isPinned(getPinnedEntitiesState(state), entityId);

//#endregion

//#region Spaces/Sources region

export const getPathFromRootEntity = (entity) =>
  /* v3 version*/ entity.get("path") ||
  /* v2 version*/ entity.get("fullPathList");
export const getNameFromPath = (path) => (path ? path.last() : null); // last element in a path is a name
export const getNameFromRootEntity = (entity) =>
  getNameFromPath(getPathFromRootEntity(entity));

function getSortedResource(resources, pinnedEntities, isV3Api) {
  const nameGetter = isV3Api
    ? (e) => getNameFromRootEntity(e)
    : (e) => e.get("name");
  return resources
    .sort((a, b) => {
      const ret =
        Number(isPinned(pinnedEntities, b.get("id")) || 0) -
        Number(isPinned(pinnedEntities, a.get("id")) || 0);
      return ret !== 0 ? ret : humanSorter(nameGetter(a), nameGetter(b));
    })
    .toList();
}

export const getSortedResourceSelector = (resourceSelectorFn, isV3Api) =>
  createSelector(
    [resourceSelectorFn, getPinnedEntitiesState],
    (spaces, pinnedEntitiesState) => {
      return getSortedResource(spaces, pinnedEntitiesState, isV3Api);
    },
  );

export const addPinStateToList = (listSelector) =>
  createSelector(
    [listSelector, getPinnedEntitiesState],
    (listOrMap, pinnedEntitiesState) => {
      invariant(
        listOrMap instanceof Immutable.List ||
          listOrMap instanceof Immutable.Map,
        "list must be of type Immutable.List or Immutable.Map",
      );
      return listOrMap
        .map((item) => {
          return item.merge({
            isActivePin: isPinned(pinnedEntitiesState, item.get("id")),
          });
        })
        .toList();
    },
  );

function getSpacesImpl(state) {
  const { entities } = state.resources;
  return entities.get(ENTITY_TYPES.space);
}

export const getSpaces = addPinStateToList(
  createSelector(getSpacesImpl, (spaceMap) =>
    spaceMap.filter((space) => space.get("containerType") === "SPACE"),
  ),
);

const getFromOptional = (obj, getterFn, defaultValue = null) => {
  invariant(getterFn, "getterFn must be provided");
  if (obj === undefined || obj === null) {
    return defaultValue;
  }
  return getterFn(obj);
};
//todo after migration of sources to v3 api, those selectors could be generalized for sources
export const getSpace = (state, spaceId) =>
  getEntity(state, spaceId, ENTITY_TYPES.space);
export const getSpaceName = (state, spaceId) =>
  getFromOptional(getSpace(state, spaceId), getNameFromRootEntity);
export const getSpaceVersion = (state, spaceId) =>
  getFromOptional(getSpace(state, spaceId), (space) => space.get("tag"));
export const getDatasetCountStats = (state, spaceId) => {
  const entity = getSpace(state, spaceId);
  let result = null;
  if (entity) {
    const stats = entity.get("stats");
    if (stats) {
      result = {
        count: stats.get("datasetCount"),
        isBounded: stats.get("datasetCountBounded"),
      };
    }
  }
  return result;
};

/**
 * Returns all space names from redux store
 * @param {*} state
 * @returns {string[]}
 */
export const getSpaceNames = (state) =>
  getSpaces(state)
    .map((space) => getNameFromRootEntity(space))
    .toList()
    .toJS();

export const getSortedSpaces = getSortedResourceSelector(getSpaces, true);

export const getSources = addPinStateToList((state) =>
  state.resources.entities.get("source"),
);

export const getSourceMap = (state) => state.resources.entities.get("source");

/**
 * Returns all source names from redux state
 * @returns {string[]}
 */
export const getSourceNames = (state) =>
  getSources(state)
    .map((source) => source.get("name"))
    .toList()
    .toJS();

export const getSortedSources = getSortedResourceSelector(getSources);

export const isHomeSource = (cur) => !!cur.get("isPrimaryCatalog");

export const getHomeSource = (sources) =>
  sources.size > 0 ? sources.find(isHomeSource) : null;

export const getHomeSourceUrl = (sources, isArsEnabled) => {
  if (!isArsEnabled) return null;

  const homeSource = getHomeSource(sources);
  if (!homeSource) return null;

  return commonPaths.source.link({
    projectId: getSonarContext()?.getSelectedProjectId?.(),
    resourceId: homeSource.get("name"),
  });
};

export const selectSourceByName = (name) => {
  return createSelector([getSortedSources], (sources) => {
    if (!sources) return null;
    return sources.toJS().find((source) => source.name === name);
  });
};

//#endregion

//#region content selectors

const contentSelector = (state) => homeSelector(state).content;
export const getCanTagsBeSkipped = (state) =>
  fromContent.getCanTagsBeSkipped(contentSelector(state));
export const getHomeEntity = (state) =>
  fromContent.getEntity(contentSelector(state));
export const getHomeEntityOrChild = (state, entityId, entityType) =>
  fromContent.getEntityOrChild(contentSelector(state), entityId, entityType);

// todo: why is this called getHomeContents - seems to do way more than "home"?
// todo: simplify this
// The only remaining places this is used are in AddFileModal, and AddFolderModal,
// which are not actually using it to get "HomeContents".
// They are just using it to get the parent entity (folder/space/source/home).
export const getHomeContents = (state) =>
  fromContent.getEntityWithContent(contentSelector(state));

//#endregion

//#region links for preview. This region works only for home space and sources
// because formatting is available only there

/**
 * Returns a root entity type.
 *
 * The method works only for sources and home space
 * @param {string[]} fullPathList - entity path
 * @returns {ENTITY_TYPES.home|ENTITY_TYPES.source} - a root entity type
 */
const getRootType = (fullPathList) => {
  invariant(
    fullPathList.length > 0,
    "fullPathList must contain at least one element",
  );
  // home space path has a first element of following format '@{user_name}'. Sources could not start
  // from '@'.
  return fullPathList[0].startsWith("@")
    ? ENTITY_TYPES.home
    : ENTITY_TYPES.source;
};

const supportedOperations = generateEnumFromList([
  "file_format",
  "folder_format", // get/save original format
  "file_preview",
  "folder_preview", // a preview for changed format
]);

/**
 * Return an url to request a current format for a file or a folder
 * @param {string[]} fullPathList
 * @param {supportedOperations} operation
 */
const buildOperationUrl = (fullPathList, operation) => {
  invariant(
    fullPathList.length > 1,
    "fullPathList must contain at least 2 elements: root source or home space and file/folder name",
  );
  invariant(
    !!supportedOperations[operation],
    `not supported operation: ${operation}`,
  );
  return (
    "/" +
    [
      getRootType(fullPathList),
      fullPathList[0],
      operation,
      ...fullPathList.slice(1),
    ]
      .map(encodeURIComponent)
      .join("/")
  );
};

/**
 * Return an url to request a current format for a file or a folder
 * @param {string[]} fullPathList
 * @param {boolean} isFolder
 */
export const getCurrentFormatUrl = (fullPathList, isFolder) => {
  return buildOperationUrl(
    fullPathList,
    isFolder ? "folder_format" : "file_format",
  );
};

/**
 * Returns an url to save a format for a file or a folder
 * @param {string[]} fullPathList
 * @param {boolean} isFolder
 */
export const getSaveFormatUrl = getCurrentFormatUrl;

/**
 * Return an url to request a preview for a new format for a file or a folder
 * @param {string[]} fullPathList
 * @param {boolean} isFolder
 */
export const getFormatPreviewUrl = (fullPathList, isFolder) => {
  return buildOperationUrl(
    fullPathList,
    isFolder ? "folder_preview" : "file_preview",
  );
};

//#endregion

/**
 * Return an url to query a file or a folder
 * works only for sources and home space {@see getRootType}
 * @param {string[]} fullPathList
 */
export const getQueryUrl = (fullPathList) => {
  invariant(
    fullPathList.length > 1,
    "fullPathList must contain at least 2 elements: root source or home space and file/folder name",
  );
  return addProjectBase(
    "/" +
      [
        getRootType(fullPathList),
        fullPathList[0],
        constructFullPath(fullPathList.slice(1)),
      ]
        .map(encodeURIComponent)
        .join("/"),
  );
};

//#region  api v3 api selectors
/**
 * Evaluates entity type from redux store, by checking whether id is presented in space, source list
 * and return 'home' as a type if entity is not a space or source
 * @param {string} entityId
 * @returns {keyof ENTITY_TYPES} - on of the following types: home, space, source
 */
export const getRootEntityTypeByIdV3 = (state, entityId) => {
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
 * @returns {Immutable.List<string>} a path for the entity
 */
const getRootEntityPathV3 = (state, entityId) => {
  const type = getRootEntityTypeByIdV3(state, entityId);
  const entity = getEntity(state, entityId, type);
  // the v2 should be removed after migration
  return type === ENTITY_TYPES.home
    ? new Immutable.List([])
    : getPathFromRootEntity(entity);
};

/**
 * Get a link url for space/source/home space
 *
 * @param {object} state
 * @param {string} entityId
 * @returns {string} a browser url to folder/space/home space
 */
export const getRootEntityLinkUrl = (state, entityId) => {
  const type = getRootEntityTypeByIdV3(state, entityId);
  const path = getRootEntityPathV3(state, entityId);
  const resourceId = path.map(encodeURIComponent).join("/");
  const projectId = getSonarContext()?.getSelectedProjectId?.();

  switch (type) {
    case ENTITY_TYPES.space:
      return commonPaths.space.link({ resourceId, projectId });
    case ENTITY_TYPES.source:
      return commonPaths.source.link({ resourceId, projectId });
    case ENTITY_TYPES.home:
    default:
      return commonPaths.home.link({ projectId });
  }
};

/**
 * works for sources, spaces and home space
 * @param {object} state
 * @param {string} entityId
 */
export const getRootEntityNameV3 = (state, entityId) => {
  const path = getRootEntityPathV3(state, entityId);
  return getNameFromPath(path);
};

//#endregion
