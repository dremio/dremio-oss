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
import { HOME_SPACE_NAME } from 'constants/Constants';

// history needs current location because its resourcePath is not enough to get client href
//eg /space/myspace

class SpaceResourcePathUtils {

  fromHref(href) {
    const m = href.match(/\/space\/(.*)/);
    if (m) {
      const resourceId = m[1];
      if (resourceId.indexOf('.') !== -1) {
        return `/folder/${resourceId}`;
      }
      return `/space/${resourceId}`;
    }
    return '/';
  }

  toFullPath(resourcePath) {
    // todo: kill this, this can't be safe
    const isNameLooksLikeHome = resourcePath && resourcePath.indexOf('.') === -1 && resourcePath.indexOf('@') !== -1;
    return resourcePath === '/' || isNameLooksLikeHome
      ? HOME_SPACE_NAME
      : resourcePath.match(/\/space\/(.*)\/list/)[1];
  }

}

const result = new SpaceResourcePathUtils();
export default result;
