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
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

// actions
const SET_ENTITY_ACTIVE_STATE = 'SET_ENTITY_ACTIVE_STATE';
export const setEntityActiveState = (id, isActive) => ({
  type: SET_ENTITY_ACTIVE_STATE,
  id,
  isActive
});

/**
 * Reducer that store active state for entities. Useful for toggles, pin states etc.
 *
 * @param {Object.<string, bool>} state<key, value> - a map that store active state for entity. A **key** is entity
 *   id, **value** - active state
 * @param {object} action
 */
export const pinnedEntities = (state = localStorageUtils.getPinnedItems(), {
  type,
  id,
  isActive
}) => {
  switch (type) {
  case SET_ENTITY_ACTIVE_STATE: {
    invariant(id, 'id must be provided');
    let newState = state;
    if (isActive) {
      newState = {
        ...state,
        [id]: true
      };
    } else if (state.hasOwnProperty(id)) { // need change the state only ifu id is presented in a state
      const {
        [id]: toRemove,
        ...rest
      } = state;
      newState = rest;
    }

    localStorageUtils.updatePinnedItemState(newState);
    return newState;
  }
  default:
    return state;
  }
};

export default pinnedEntities;

// selectors
export const isPinned = (state, entityId) => !!state[entityId];
