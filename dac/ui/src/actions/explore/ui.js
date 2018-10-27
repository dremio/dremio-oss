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
export const SET_SQL_EDITOR_SIZE = 'SET_SQL_EDITOR_SIZE';

export function updateSqlPartSize(size) {
  return { type: SET_SQL_EDITOR_SIZE, size };
}

export const  TOGGLE_EXPLORE_SQL = 'TOGGLE_EXPLORE_SQL';

export function toggleExploreSql() {
  return {type: TOGGLE_EXPLORE_SQL};
}

export const COLLAPSE_EXPLORE_SQL = 'COLLAPSE_EXPLORE_SQL';

export const collapseExploreSql = () => {
  return {type: COLLAPSE_EXPLORE_SQL};
};

export const  EXPAND_EXPLORE_SQL = 'EXPAND_EXPLORE_SQL';

export function expandExploreSql() {
  return {type: EXPAND_EXPLORE_SQL};
}

export const  RESIZE_PROGRESS_STATE = 'RESIZE_PROGRESS_STATE';

export function setResizeProgressState(state) {
  return {type: RESIZE_PROGRESS_STATE, state};
}

export const SHOW_QLIK_ERROR = 'SHOW_QLIK_ERROR';
export const HIDE_QLIK_ERROR = 'HIDE_QLIK_ERROR';

export function showQlikError(error) {
  return {
    type: SHOW_QLIK_ERROR,
    payload: {
      error
    }
  };
}

export function hideQlikError() {
  return {type: HIDE_QLIK_ERROR};
}

export const SHOW_QLIK_MODAL = 'SHOW_QLIK_MODAL';
export const HIDE_QLIK_MODAL = 'HIDE_QLIK_MODAL';

export function showQlikModal(dataset) {
  return {
    type: SHOW_QLIK_MODAL,
    dataset
  };
}

export function hideQlikModal() {
  return {
    type: HIDE_QLIK_MODAL
  };
}

export const SHOW_QLIK_PROGRESS = 'SHOW_QLIK_PROGRESS';
export const HIDE_QLIK_PROGRESS = 'HIDE_QLIK_PROGRESS';

export function showQlikProgress() {
  return {type: SHOW_QLIK_PROGRESS};
}

export function hideQlikProgress() {
  return {type: HIDE_QLIK_PROGRESS};
}
