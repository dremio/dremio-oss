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
export const UPDATE_GRID_SIZES = 'UPDATE_GRID_SIZES';

export function updateGridSizes(sizes) {
  return (dispatch) => {
    const action = { type: UPDATE_GRID_SIZES, sizes };
    dispatch(action);
  };
}

export const UPDATE_RIGHT_PANEL_VISIBILITY = 'UPDATE_RIGHT_PANEL_VISIBILITY';
export function updateRightTreeVisibility(isVisible) {
  return (dispatch) => {
    const action = { type: UPDATE_RIGHT_PANEL_VISIBILITY, visible: isVisible };
    dispatch(action);
  };
}

export const UPDATE_RESOURCE_TREE_PATH = 'UPDATE_RESOURCE_TREE_PATH';
export function updateResourceTreePath(path) {
  return (dispatch) => {
    const action = { type: UPDATE_RESOURCE_TREE_PATH, path };
    dispatch(action);
  };
}

export const TOGGLE_RESOURCE_TREE_NODE_EXPANDED = 'TOGGLE_RESOURCE_TREE_NODE_EXPANDED';
export function toggleResourceTreeNodeExpanded(id) {
  return (dispatch) => {
    const action = { type: TOGGLE_RESOURCE_TREE_NODE_EXPANDED, id };
    dispatch(action);
  };
}

export const RESET_RESOURCE_TREE = 'RESET_RESOURCE_TREE';
export function resetResourceTree() {
  return (dispatch) => {
    const action = { type: RESET_RESOURCE_TREE };
    dispatch(action);
  };
}
