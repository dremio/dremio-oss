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

import * as ActionTypes from "actions/resources/scripts";

type ScriptsState = {
  all: any[];
  mine: any[];
};

type ScriptActions = {
  type: string;
  payload: any;
  meta: any;
};

function getInitialState(): ScriptsState {
  return {
    all: [],
    mine: [],
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
    default:
      return state;
  }
}
