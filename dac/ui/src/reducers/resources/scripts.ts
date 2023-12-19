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
// import Immutable from 'immutable';

import * as ActionTypes from "@app/actions/resources/scripts";

type ScriptsState = {
  all: any[];
  mine: any[];
  scriptsSyncPending: Set<string>;
};

type ScriptActions = {
  type: string;
  payload: any;
  meta: any;
} | {
  type: "SCRIPT_SYNC_STARTED";
  id: string;
} | {
  type: "SCRIPT_SYNC_COMPLETED";
  id: string;
};

function getInitialState(): ScriptsState {
  return {
    all: [],
    mine: [],
    scriptsSyncPending: new Set()
  };
}

export default function scripts(
  state = getInitialState(),
  action: ScriptActions
): ScriptsState {
  const data =
    action && action.payload && action.payload.data ? action.payload.data : [];
  switch (action.type) {
    case ActionTypes.FETCH_SCRIPTS_SUCCESS:
      return { ...state, all: data };
    case ActionTypes.FETCH_MINE_SCRIPTS_SUCCESS:
      return { ...state, mine: data };
    case ActionTypes.REPLACE_SCRIPT_CONTENTS: {
      const scriptAllIndex = state.all.findIndex(script => script.id === action.scriptId);
      const scriptMineIndex = state.mine.findIndex(script => script.id === action.scriptId);

      if (scriptAllIndex === -1 && scriptMineIndex === -1) {
        return state;
      }

      let nextAll = state.all;
      let nextMine = state.mine;

      if (scriptAllIndex >= 0) {
        nextAll = [...state.all];
        nextAll[scriptAllIndex] = action.script;
      }

      if (scriptMineIndex >= 0) {
        nextMine = [...state.mine];
        nextMine[scriptMineIndex] = action.script;
      }

      return { ...state, all: nextAll, mine: nextMine }
    }
    case "SCRIPT_SYNC_STARTED": {
      const nextState = new Set(state.scriptsSyncPending);
      nextState.add(action.id);
      return {
        ...state,
        scriptsSyncPending: nextState
      }
    }

    case "SCRIPT_SYNC_COMPLETED": {
      const nextState = new Set(state.scriptsSyncPending);
      nextState.delete(action.id);
      return {
        ...state,
        scriptsSyncPending: nextState
      }
    }
    default:
      return state;
  }
}
