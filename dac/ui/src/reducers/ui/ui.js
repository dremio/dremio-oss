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

import * as ActionTypes from 'actions/ui/ui';

const initialState = Immutable.fromJS({
  gridPageSizes: {
    gridPageWidth: 0,
    gridPageHeight: 0,
    gridTableMargin: 0,
    gridTableHeight: 0,
    historyLineWidth: 0
  },
  rightTreeVisible: false,
  resourceTree: {
    path: [],
    nodes: {}
  }
});

export default function ui(state = initialState, action) {
  switch (action.type) {
  case ActionTypes.UPDATE_GRID_SIZES:
    return state.set('gridPageSizes', action.sizes);

  case ActionTypes.UPDATE_RIGHT_PANEL_VISIBILITY:
    return state.set('rightTreeVisible', action.visible);

  case ActionTypes.RESET_RESOURCE_TREE:
    return state.set('resourceTree', Immutable.Map());
  case ActionTypes.UPDATE_RESOURCE_TREE_PATH:
    return state.setIn('resourceTree', 'path', action.path);
  case ActionTypes.TOGGLE_RESOURCE_TREE_NODE_EXPANDED: {
    const expanded = state.getIn(['resourceTree', 'nodes', action.id, 'expanded']);
    return state.mergeIn(['resourceTree', 'nodes', action.id], {expanded: !expanded});
  }
  default:
    return state;
  }
}

export function getResourceTreePath(state) {
  return state.ui.getIn('resourceTree', 'path');
}
