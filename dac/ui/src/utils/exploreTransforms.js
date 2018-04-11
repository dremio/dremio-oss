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
const OPTIMISTIC_TRANSFORMS = ['DROP', 'RENAME', 'DESC', 'ASC', 'MULTIPLY'];

class Transforms {
  isTransformOptimistic(type) {
    return OPTIMISTIC_TRANSFORMS.indexOf(type) !== -1;
  }

  sort({table}) {
    return table;
  }

  dropColumn({name, table}) {
    const nextColumns = table.get('columns').filter(col => col.get('name') !== name);
    const colIndex = table.get('columns').findIndex(col => col.get('name') === name);
    const nextRows = table.get('rows').filter((r, index) => index !== colIndex);
    return table.set('columns', nextColumns).set('rows', nextRows);
  }

  renameColumn({name, nextName, table}) {
    const index = table.get('columns').findIndex(col => col.get('name') === name);
    return table.setIn(['columns', index, name], nextName);
  }
}

export default (new Transforms());
