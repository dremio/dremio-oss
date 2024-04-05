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

import { EXPLORE_PAGE_LOCATION_CHANGED } from "@app/actions/explore/dataset/data";
import { extractValue } from "@app/reducers/reducerFactories";
import graph from "dyn-load/reducers/explore/graph";

import sqlActions from "./sqlActions";
import recommended from "./recommended";
import join from "./join";
import ui from "./ui";
import view from "./view";
import { tabViewReducer } from "./tabViewReducer";
import exploreJobs from "./exploreJobs";
import { tabExploreJobsReducer } from "./tabExploreJobs";
import { SET_TAB_VIEW } from "@app/actions/resources/scripts";
import { cloneDeep } from "lodash";
import { RESET_QUERY_STATE } from "@app/actions/explore/view";
import exploreUtils from "@app/utils/explore/exploreUtils";

// export for testing
export const currentRouteState = extractValue(
  EXPLORE_PAGE_LOCATION_CHANGED,
  "newRouteState",
);

export default function exploreReducer(curState, action) {
  const state = combineReducers({
    view,
    tabViews: (...args) => {
      return tabViewReducer({ view: view(curState?.view, action) })(...args);
    },
    exploreJobs,
    // might later be able to consolidate with tabViews
    tabExploreJobs: (...args) => {
      return tabExploreJobsReducer({
        exploreJobs: exploreJobs(curState?.exploreJobs, action),
      })(...args);
    },
    sqlActions,
    ui,
    graph,
    recommended,
    join,
    currentRouteState,
  })(curState, action);

  if (action.type === SET_TAB_VIEW) {
    const {
      script: { id: scriptId },
    } = action;
    if (!scriptId) {
      return state;
    }

    const hasState = state.tabViews[scriptId];
    if (hasState) {
      const queryStatuses = state.tabViews[scriptId].queryStatuses || [];

      const result = {
        ...state,
        view: {
          ...cloneDeep(state.tabViews[scriptId]),
          queryStatuses: queryStatuses.map((status) => {
            const isCancellable = exploreUtils.getCancellable(status);
            return { ...status, cancelled: isCancellable };
          }),
          isMultiQueryRunning: false,
          queryFilter: "",
        },
        ...(state.tabExploreJobs[scriptId] && {
          exploreJobs: {
            ...cloneDeep(state.tabExploreJobs[scriptId]),
          },
        }),
      };
      return result;
    } else {
      //Initialize a new view state with the correct script
      const result = {
        ...state,
        view: {
          ...view(view, { type: RESET_QUERY_STATE }),
          queryFilter: "", // Reset query state should prob do this
          activeScript: action.script,
          currentSql: action.script.content,
          previousMultiSql: action.script.content,
          queryTabNumber: 1,
          queryContext: state.view.queryContext,
        },
        exploreJobs: state.exploreJobs,
      };

      return result;
    }
  }

  return state;
}
