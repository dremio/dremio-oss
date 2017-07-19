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
class PageType {
  getPageType() {
    const pathname = window.location.pathname;
    const pattern = pathname.match('/home/(.*)/');
    if (pattern !== null) {
      return pattern[1];
    } else if (pathname === '/' ||
      (pathname.indexOf('/home/@') > -1 && pathname.indexOf('.') === -1)) {
      return 'home';
    }
    return '';
  }

  getResourceType(pathname) {
    if (pathname.indexOf('/source/') !== -1) {
      return 'source';
    } else if (pathname.indexOf('/space/') !== -1) {
      return 'space';
    } else if (pathname.indexOf('/home') !== -1) {
      return 'home';
    }
  }
}

const pageType = new PageType();

export default pageType;
