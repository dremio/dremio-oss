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

import * as ActionTypes from 'actions/resources/sourceDetails';
import * as AllSourcesActionTypes from 'actions/resources/sources';
import homeMapper from 'utils/mappers/homeMapper';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

export const CREATED_SOURCE_NAME = '`createdSource`'; // todo: this is also used for source *editting*?
const ENTITY_NAME = 'sources';

function getInitialState() {
  return Immutable.fromJS({
    sources: [],
    sourcesById: {...localStorageUtils.getPinnedItems()[ENTITY_NAME]}
  });
}

export default function sourceList(state = getInitialState(), action) {
  switch (action.type) {
  case AllSourcesActionTypes.GET_CREATED_SOURCE_START:
    return state.set(CREATED_SOURCE_NAME, Immutable.Map({isInProgress: true}));
  case AllSourcesActionTypes.GET_CREATED_SOURCE_SUCCESS: {
    const decoratedSource = homeMapper.decorateSource(Immutable.fromJS(action.payload));
    const source = state.get(CREATED_SOURCE_NAME).merge(decoratedSource);
    return state.set(CREATED_SOURCE_NAME, source);
  }
  case AllSourcesActionTypes.SOURCES_LIST_LOAD_START: {
    return state.set('isInProgress', true);
  }
  case AllSourcesActionTypes.REMOVE_SOURCE_START: {
    return state.delete(action.meta.name);
  }
  case AllSourcesActionTypes.REMOVE_SOURCE_SUCCESS: {
    const newSpaces = state.get('sources').filter((value) => {
      return value !== action.meta.id;
    });

    return state.set('sources', newSpaces);
  }
  case AllSourcesActionTypes.SOURCES_LIST_LOAD_SUCCESS: {
    return state.set('sources', action.payload.getIn(['result', 'sources']))
      .set('isInProgress', false);
  }
  case ActionTypes.LOAD_SOURCE_STARTED: {
    if (state.get(action.meta.resourceId)) {
      return state.setIn([action.meta.resourceId, 'isInProgress'], true);
    }
    return state;
  }
  default:
    return state;
  }
}
