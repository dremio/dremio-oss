/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import * as ActionTypes from "actions/supportFlags";

const initialState = {
  "ui.upload.allow": false,
  "client.tools.tableau": false,
  "client.tools.powerbi": false,
};

export default function supportFlags(state = initialState, action) {
  const { type, payload } = action;

  switch (type) {
    case ActionTypes.SUPPORT_FLAG_SUCCESS: {
      state[`${payload.id}`] = payload.value;
      localStorage.setItem("supportFlags", JSON.stringify(state));
      // for edge case when value is set, it still shows suggestions
      if (payload.id === "ui.autocomplete.allow" && !payload.value) {
        localStorage.removeItem("isAutocomplete");
      }
      return { ...state };
    }
    case ActionTypes.SUPPORT_FLAG_FAILURE: {
      return state;
    }
    case ActionTypes.SAVE_SUPPORT_FLAG_SUCCESS: {
      state[`${payload.id}`] = payload.value;
      localStorage.setItem("supportFlags", JSON.stringify(state));
      return { ...state };
    }
    default:
      return state;
  }
}
