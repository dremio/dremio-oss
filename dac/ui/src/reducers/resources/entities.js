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
import Immutable from 'immutable';

import entityTypes from 'dyn-load/reducers/resources/entityTypes';
import { LOAD_ENTITIES_SUCCESS } from '@app/actions/resources';

import * as entityReducers from './entityReducers';

export const cacheConfigs = {
  tableData: {
    max: 20
  }
};

const initialState = entityTypes.reduce(
  (prevMap, entityType) => {
    return prevMap.set(entityType, cacheConfigs[entityType] ? Immutable.OrderedMap() : Immutable.Map());
  },
  Immutable.Map()
);

export function evictOldEntities(entities, max) {
  if (entities.size > max) {
    return entities.slice(entities.size - max);
  }
  return entities;
}

const applyEntitiesToState = (state, action) => {
  let result = state;
  const applyMethod = action.meta && action.meta.mergeEntities ? 'mergeIn' : 'setIn';
  action.payload.get('entities').forEach((entitiesToAdd, entityType) => {
    entitiesToAdd.forEach((entity, entityId) => {
      result = result[applyMethod]([entityType, entityId], entity);
    });
    if (cacheConfigs[entityType]) {
      result = result.set(entityType, evictOldEntities(result.get(entityType), cacheConfigs[entityType].max));
    }
  });
  return result;
};

const isActionWithEntities = (action) =>
  (!action.meta || !action.meta.ignoreEntities) &&
  action.payload && action.payload.get && action.payload.get('entities');

const clearEntitiesByType = (state, types) => {
  let nextState = state;

  if (types) {
    for (const entityType of types) {
      nextState = nextState.set(entityType, new Immutable.Map());
    }
  }

  return nextState;
};

export default function entitiesReducer(state = initialState, action) {
  let nextState = state;

  // DX-10700 clear data that could be cached for other folders. New data would be applied below
  if (action.type === LOAD_ENTITIES_SUCCESS) {
    nextState = clearEntitiesByType(nextState, ['folder', 'file', 'fileFormat']);
  }

  if (action.meta) {

    //todo add description of setting bellow
    const {
      entityRemovePaths,
      entityClears,
      emptyEntityCache
    } = action.meta;

    if (entityRemovePaths) {
      for (const path of entityRemovePaths) {
        nextState = nextState.deleteIn(path);
      }
    }

    nextState = clearEntitiesByType(nextState, entityClears);

    clearEntitiesByType(entityClears);

    if (emptyEntityCache) {
      // Remove folders when certain entities (like source or space) are removed to avoid caching outdated data
      const root = emptyEntityCache;
      const newFolders = nextState.get('folder').filter((folder) => {
        return folder.get('fullPathList').get(0) !== root;
      });

      const newFiles = nextState.get('file').filter((file) => {
        return file.get('fullPathList').get(0) !== root;
      });

      const newFileFormats = nextState.get('fileFormat').filter((fileFormat) => {
        return fileFormat.get('fullPath').get(0) !== root;
      });

      nextState = nextState.set('folder', newFolders).set('file', newFiles).set('fileFormat', newFileFormats);
    }
  }

  nextState = isActionWithEntities(action)
    ? applyEntitiesToState(nextState, action)
    : nextState;

  // why do we apply reducer to whole state not to prevState[key]???
  return Object.keys(entityReducers).reduce((prevState, key) => entityReducers[key](prevState, action), nextState);
}
