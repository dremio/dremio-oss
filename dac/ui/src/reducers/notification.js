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
import * as ActionTypes from 'actions/notification';
import actionUtils from 'utils/actionUtils/actionUtils';

// Every state change here gets picked up by containers/Notifications and shown.
// Only change state if you want to show a notification.
export default function notification(state = {}, action) {
  if (action.meta && action.meta.notification) {
    if (action.meta.notification === true) {
      // no error, do nothing
      if (!action.error) {
        return state;
      }
      return actionUtils.humanizeNotificationMessage()(action.payload);
    }
    if (typeof action.meta.notification === 'function') {
      return action.meta.notification(action.payload);
    }
    // replace the notification state
    return action.meta.notification;
  }
  switch (action.type) {
  case ActionTypes.ADD_NOTIFICATION:
    return {
      message: action.message,
      level: action.level,
      autoDismiss: action.autoDismiss
    };
  case ActionTypes.REMOVE_NOTIFICATION:
    return {
      removeMessageType: action.messageType
    };
  default:
    return state;
  }
}
