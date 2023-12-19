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

import { cloneDeep } from "lodash";
import viewReducer from "./view";
import { SET_TAB_VIEW, REMOVE_TAB_VIEW } from "@app/actions/resources/scripts";

export const tabViewReducer =
  (context) =>
  (curState = {}, action) => {
    const { view } = context;
    if (!view) return curState;

    let state = curState;

    // Store old tab
    if (action.type === SET_TAB_VIEW) {
      const { prevScript } = action;
      if (prevScript?.id) {
        state[prevScript.id] = cloneDeep(view);
      }
    } else if (action.type === REMOVE_TAB_VIEW) {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { [action.scriptId]: toRemove, ...rest } = state;
      return rest;
    }

    if (action.tabId) {
      if (!state[action.tabId]) {
        return state;
      }

      const newAction = {
        ...action,
        tabId: null, // Run the action
      };

      state = {
        ...state,
        [action.tabId]: {
          ...viewReducer(state[action.tabId], newAction),
        },
      };
    }

    return state;
  };
