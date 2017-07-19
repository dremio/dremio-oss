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
import Immutable from 'immutable';

import * as AllSpacesActionTypes from 'actions/resources/spaces';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

const ENTITY_NAME = 'spaces';

function getInitialState() {
  return Immutable.fromJS({
    spaces: [],
    spacesById: { ...localStorageUtils.getPinnedItems()[ENTITY_NAME] }
  });
}

export default function spaceList(state = getInitialState(), action) {
  switch (action.type) {
  case AllSpacesActionTypes.SET_SPACE_PIN_STATE: {
    localStorageUtils.updatePinnedItemState(action.name, { isActivePin: action.isActivePin }, ENTITY_NAME);
    return state.setIn(['spacesById', action.name, 'isActivePin'], action.isActivePin);
  }
  case AllSpacesActionTypes.SPACES_LIST_LOAD_SUCCESS: {
    return state.set('spaces', action.payload.getIn(['result', 'spaces']));
  }
  default:
    return state;
  }
}
