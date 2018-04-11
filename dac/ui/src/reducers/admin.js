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

import * as ActionTypes from 'actions/admin';
import adminMapper from 'utils/mappers/adminMapper';
import { LOAD_ACCELERATIONS_SUCCESS, DELETE_ACCELERATION_SUCCESS } from 'actions/resources/acceleration';

const initialState = Immutable.fromJS({
  accelerations: [],
  sourceNodesList: {
    nodes: [],
    isInProgress: false,
    isFailed: false
  },
  users: [],
  groups: [],
  userData: Immutable.Map() // current logged in user
});

export default function admins(state = initialState, action) {

  switch (action.type) {
  case ActionTypes.SOURCE_NODES_START:
    return state.set('sourceNodesList', Immutable.fromJS({
      nodes: []
    }));

  case ActionTypes.SOURCE_NODES_SUCCESS:
    return state.set('sourceNodesList', Immutable.fromJS({
      nodes: adminMapper.mapSourceNodesList(action.payload)
    }));

  case ActionTypes.LOAD_FILTERED_USER_SUCCESS:
    return state.set('users', action.payload.getIn(['result', 'users']));


  case ActionTypes.LOAD_FILTERED_GROUP_SUCCESS:
    return state.set('groups', action.payload.getIn(['result', 'groups']));

  case LOAD_ACCELERATIONS_SUCCESS:
    return state.set('accelerations', action.payload.getIn(['result', 'accelerationList']));

  case DELETE_ACCELERATION_SUCCESS:
    return state.set('accelerations', state.get('accelerations').filter(id => id !== action.meta.accelerationId));

  default:
    return state;
  }
}
