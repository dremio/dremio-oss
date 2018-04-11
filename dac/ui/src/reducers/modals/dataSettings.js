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

import * as ActionTypes from 'actions/modals/dataSettings';

const initialState = Immutable.fromJS({
  acceleration: {
    historyItems: [],
    enabled: false
  },
  accelerationSettings: {}
});

export default function dataSettings(state = initialState, action) {
  switch (action.type) {
  // todo: replace more of this with ViewStateWrapper
  case ActionTypes.ACCELERATION_DATA_START:
    return state.set('acceleration', Immutable.fromJS({
      historyItems: [],
      enabled: false
    }));
  case ActionTypes.ACCELERATION_DATA_SUCCESS:
    return state.set('acceleration', Immutable.fromJS({
      historyItems: action.payload.infosList, // todo: naming normalization (server-side?)
      enabled: action.payload.enabled
    }));
  case ActionTypes.ACCELERATION_UPDATE_START:
    return state.set('acceleration', Immutable.fromJS({
      historyItems: state.get('acceleration').get('historyItems'),
      enabled: state.get('acceleration').get('enabled')
    }));
  case ActionTypes.ACCELERATION_UPDATE_SUCCESS:
    return state.set('acceleration', Immutable.fromJS({
      historyItems: state.get('acceleration').get('historyItems'),
      enabled: state.get('acceleration').get('enabled')
    }));
  case ActionTypes.ACCELERATION_SETTINGS_SUCCESS:
    return state.set('accelerationSettings', Immutable.fromJS(action.payload));
  default:
    return state;
  }
}
