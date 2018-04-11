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
import { ACCESS_ENTITY } from 'actions/resources/lru';

export default function table(state, action) {
  switch (action.type) {
  case ACCESS_ENTITY: {
    const { entityType, entityId } = action.meta;
    const entity = state.getIn([entityType, entityId]);
    if (!entity) {
      return state;
    }
    // delete/set moves to front of ordered map
    return state.deleteIn([entityType, entityId]).setIn([entityType, entityId], entity);
  }
  default:
    return state;
  }
}
