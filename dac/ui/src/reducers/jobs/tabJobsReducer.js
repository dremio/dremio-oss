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
import { SET_TAB_VIEW, REMOVE_TAB_VIEW } from "@app/actions/resources/scripts";
import jobs from "./jobs";

/**
 *
 * Processes actions with a tabId and applies them to the correct state in the map/
 * This relies on the job reducer to skip these actions when the tabId is defined.
 *
 * TabId is omitted when the calling the jobs reducer for the items in the state map so that they won't be skipped.
 * This allows sharing the job reducer for the namespaced state
 */
export const tabJobsReducer =
  (context) =>
  (curState = {}, action) => {
    let state = curState;

    if (action.type === SET_TAB_VIEW) {
      const {
        prevScript: { id: prevScriptId },
      } = action;
      if (!prevScriptId) return state;

      state[prevScriptId] = cloneDeep(context.jobs);
    } else if (action.type === REMOVE_TAB_VIEW) {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { [action.scriptId]: toRemove, ...rest } = state;
      return rest;
    }

    const keys = Object.keys(state);
    if (keys.length > 0) {
      return keys.reduce((acc, curKey) => {
        if (action.tabId) {
          acc[curKey] = jobs(state[curKey], { ...action, tabId: null });
        } else {
          acc[curKey] = state[curKey];
        }
        return acc;
      }, state);
    } else {
      return state;
    }
  };
