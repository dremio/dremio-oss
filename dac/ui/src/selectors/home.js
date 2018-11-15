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
import {getUserName} from 'selectors/account';
import { getHomePageEntity } from 'selectors/datasets';
import * as fromWiki from '@app/reducers/home/wiki';

const rootSelector = state => state.home;

export function getHomeForCurrentUser(state) {
  const userName = getUserName(state);

  const {entities} = state.resources;

  const entry = entities.get('home') && entities.get('home').findEntry((home) => home.get('owner') === userName);
  if (entry) {
    return entry[1];
  }
  return Immutable.fromJS({
    id: '/home/"@' + userName + '"',
    fullPathList: ['@' + userName],
    links: {
      self: '/home/"@' + userName + '"'
    }
  });
}

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
