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
import invariant from 'invariant';

const validTypes = ['dimensions', 'measures', 'columnsList'];

export default class ColumnDragItem {
  constructor(id = null, dragOrigin = null) {
    invariant(dragOrigin === null || validTypes.includes(dragOrigin),
      'Drag origin should be one of the valid types');

    this.id = id;
    this.dragOrigin = dragOrigin;
  }
}
