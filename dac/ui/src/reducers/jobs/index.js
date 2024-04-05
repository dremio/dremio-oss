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
import { combineReducers } from "redux";

import jobs from "./jobs";
import { SET_TAB_VIEW } from "@app/actions/resources/scripts";
import { tabJobsReducer } from "./tabJobsReducer";
import { cloneDeep } from "lodash";

const jobsReducer = (curState, action) => {
  const state = combineReducers({
    jobs,
    // TODO: goal is to ultimately remove this from jobs in redux and move it to the explore module
    tabJobs: (...args) => {
      return tabJobsReducer({
        jobs: jobs(curState?.jobs, action),
      })(...args);
    },
  })(curState, action);

  if (action.type === SET_TAB_VIEW) {
    const {
      script: { id: scriptId },
    } = action;
    const tabJobs = state.tabJobs?.[scriptId];
    if (!tabJobs) return state;

    return {
      ...state,
      jobs: cloneDeep(tabJobs),
    };
  }

  return state;
};

export default jobsReducer;
