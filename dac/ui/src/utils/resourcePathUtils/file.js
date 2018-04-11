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
//eg /source/mongo/dataset/mongo.academic.business

class FileResourcePathUtils {
  toFullPath(resourcePath) {
    const parts = resourcePath.split('/');
    if (parts.length === 3) {
      return parts[2];
    }
    return parts[4];
  }

  toHref(resourcePath) {
    const parts = resourcePath.split('/');
    const fullPath = this.toFullPath(resourcePath);
    const fullPathParts = fullPath.match(/([^".]+|"[^"]+")/g);
    let name = fullPathParts[fullPathParts.length - 1];
    if (name && name[0] === '"') {
      const m = name.match(/"(.+)"/);
      if (m) {
        name = m[1];
      }
    }

    const suffix =
        `${fullPathParts.slice(0, fullPathParts.length - 1).join('.')}/${name}`;

    if (parts.length === 3) {
      return `/space/${suffix}`;
    }
    return `/source/${suffix}`;
  }
}

const result = new FileResourcePathUtils();
export default result;
