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
import moment from 'moment';
import * as ActionTypes from 'actions/serverStatus';
import socket, {WS_CONNECTION_OPEN, WS_CONNECTION_CLOSE} from 'utils/socket';

import config from 'utils/config';

const initialState = Immutable.Map({
  status: config.serverStatus,
  socketIsOpen: false
});

export default function serverStatus(state = initialState, action) {

  switch (action.type) {
  case ActionTypes.SCHEDULE_CHECK_SERVER_STATUS:
    return state.filter((v, k) => k === 'status');
  case ActionTypes.CHECK_SERVER_STATUS_START:
    if (!action.error) {
      const result = state.set('lastCheckMoment', moment());
      if (action.meta.delay) {
        return result.set('delay', action.meta.delay);
      }
      return result;
    }
    return state;
  case ActionTypes.CHECK_SERVER_STATUS_SUCCESS:
    return state.set('status', action.payload);
  case WS_CONNECTION_OPEN:
  case WS_CONNECTION_CLOSE:
    return state.set('socketIsOpen', socket.isOpen);
  default:
    return state;
  }
}
