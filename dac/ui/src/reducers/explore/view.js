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
import Immutable  from 'immutable';

import * as ActionTypes from 'actions/explore/view';

import { RUN_TABLE_TRANSFORM_START, RUN_TABLE_TRANSFORM_SUCCESS } from 'actions/explore/dataset/common';
import { LOAD_EXPLORE_ENTITIES_SUCCESS } from 'actions/explore/dataset/get';

export const EXPLORE_VIEW_ID = 'EXPLORE_VIEW_ID';

const initialState = Immutable.fromJS({
  queryContext: Immutable.List(),
  currentSql: undefined, // currentSql === undefined means sql has not been changed
  sqlBoxSize: 0,
  isResizeInProgress: false,
  isTransformWarningModalVisible: false,
  tables: {},
  isPreviewMode: true,
  sqlEditorFocusKey: 0 // number
});

export default function view(state = initialState, action) {
  switch (action.type) {

  case ActionTypes.SET_CURRENT_SQL: {
    return state.set('currentSql', action.sql);
  }

  case ActionTypes.SET_QUERY_CONTEXT: {
    return state.set('queryContext', action.context);
  }

  case ActionTypes.RESET_NEW_QUERY: {
    return state.set('currentSql', undefined);
  }

  case RUN_TABLE_TRANSFORM_START: {
    return state.setIn(['transform', action.meta.href], Immutable.Map());
  }

  case LOAD_EXPLORE_ENTITIES_SUCCESS:
  case RUN_TABLE_TRANSFORM_SUCCESS: {
    const version = action.payload && action.payload.get && action.payload.get('result');
    const tables = action.payload && action.payload.getIn && action.payload.getIn(['entities', 'table']);
    const nextState = state.set('activeTransformationHref', '');
    if (!tables) {
      return nextState;
    }
    return nextState.setIn(['tables', version, 'columns'], tables.getIn([version, 'columns']));
  }

  case ActionTypes.FOCUS_EDITOR: {
    return state.set('sqlEditorFocusKey', (new Date()).getTime()); // todo replace with a key provider as reducer should be a pure function
  }

  default:
    return state;
  }
}
