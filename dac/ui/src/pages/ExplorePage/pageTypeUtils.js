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
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';

export const getPathPart = pageType => pageType && pageType !== PageTypes.default ? `/${pageType}` : '';

const countSlashes = str => {
  if (!str) return 0;
  const matches = str.match(/\//g);
  return matches ? matches.length : 0;
};
// explore page has the following url pattern (see routes.js):
// So page type may or may not be presented.
const patternSlashCount = countSlashes('/resources/resourceId/tableId(/:pageType)');

export const excludePageType = pathname => {
  const slashCount = countSlashes(pathname);
  let pathWithoutPageType = pathname;
  if (slashCount === patternSlashCount) { // current path contains pageType. We should exclude it
    pathWithoutPageType = pathname.substr(0, pathname.lastIndexOf('/'));
  }
  return pathWithoutPageType;
};

// query parameters will removed from existing url
export const changePageTypeInUrl = (pathname, newPageType) => {
  return excludePageType(pathname) + getPathPart(newPageType);
};
