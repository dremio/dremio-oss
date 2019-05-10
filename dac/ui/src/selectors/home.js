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
import invariant from 'invariant';
import { getHomePageEntity } from 'selectors/datasets';
import * as fromWiki from '@app/reducers/home/wiki';
import { isPinned }  from '@app/reducers/home/pinnedEntities';
import { humanSorter } from 'utils/sort';

const rootSelector = state => state.home;

export const isWikiPresent = (state, entityId) => !!fromWiki.getWiki(rootSelector(state).wiki, entityId);
export const isWikiLoaded = (state, entityId) => fromWiki.isWikiLoaded(rootSelector(state).wiki, entityId);
export const getWikiValue = (state, entityId) => fromWiki.getWiki(rootSelector(state).wiki, entityId);
export const getWikiVersion = (state, entityId) => fromWiki.getWikiVersion(rootSelector(state).wiki, entityId);
export const isWikiLoading = (state, entityId) => fromWiki.isWikiLoading(rootSelector(state).wiki, entityId);
export const getErrorInfo = (state, entityId) => fromWiki.getErrorInfo(rootSelector(state).wiki, entityId);
export const getCanTagsBeSkipped = (state, urlPath) => {
  const entity = getHomePageEntity(state, urlPath);
  return entity ? entity.getIn(['contents', 'canTagsBeSkipped'], false) : false;
};
export const getSidebarSize = (state) => rootSelector(state).sidebarSize;
export const getPinnedEntitiesState = state => rootSelector(state).pinnedEntities;
export const isEntityPinned = (state, entityId) => isPinned(getPinnedEntitiesState(state), entityId);

function getSortedResource(resources, pinnedEntities) {
  return resources.sort((a, b) => {
    const ret = Number(isPinned(pinnedEntities, b.get('id')) || 0) - Number(isPinned(pinnedEntities, a.get('id')) || 0);
    return ret !== 0 ? ret : humanSorter(a.get('name'), b.get('name'));
  }).toList();
}

export const getSortedResourceSelector = resourceSelectorFn => createSelector(
  [ resourceSelectorFn, getPinnedEntitiesState ],
  (spaces, pinnedEntitiesState) => {
    return getSortedResource(spaces, pinnedEntitiesState);
  }
);

export const addPinStateToList = listSelector => createSelector(
  [ listSelector, getPinnedEntitiesState ],
  (list, pinnedEntitiesState) => {
    invariant(list instanceof Immutable.List, 'list must be of type Immutable.List');
    return list.map(item => {
      return item.merge({
        isActivePin: isPinned(pinnedEntitiesState, item.get('id'))
      });
    });
  }
);
