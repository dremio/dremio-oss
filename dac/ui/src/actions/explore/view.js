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
import Immutable from "immutable";
import actionUtils from "utils/actionUtils/actionUtils";

export const SET_CURRENT_SQL = "SET_CURRENT_SQL";
export const SET_PREVIOUS_MULTI_SQL = "SET_PREVIOUS_MULTI_SQL";
export const SET_PREVIOUS_AND_CURRENT_SQL = "SET_PREVIOUS_AND_CURRENT_SQL";
export const SET_SELECTED_SQL = "SET_SELECTED_SQL";
export const SET_UPDATE_SQL_FROM_HISTORY = "SET_UPDATE_SQL_FROM_HISTORY";
export const SET_IS_MULTI_QUERY_RUNNING = "SET_IS_MULTI_QUERY_RUNNING";
export const SET_QUERY_CONTEXT = "SET_QUERY_CONTEXT";
export const SET_QUERY_STATUSES = "SET_QUERY_STATUSES";
export const SET_QUERY_TAB_NUMBER = "SET_QUERY_TAB_NUMBER";
export const SET_QUERY_SELECTIONS = "SET_QUERY_SELECTIONS";
export const SET_QUERY_FILTER = "SET_QUERY_FILTER";
export const RESET_NEW_QUERY = "RESET_NEW_QUERY";
export const RESET_QUERY_STATE = "RESET_QUERY_STATE";
export const FOCUS_EDITOR = "FOCUS_SQL_EDITOR";

export function setCurrentSql({ sql }) {
  return { type: SET_CURRENT_SQL, sql };
}

export function setPreviousMultiSql({ sql }) {
  return { type: SET_PREVIOUS_MULTI_SQL, sql };
}

export function setPreviousAndCurrentSql({ sql }) {
  return { type: SET_PREVIOUS_AND_CURRENT_SQL, sql };
}

export function setSelectedSql({ sql }) {
  return { type: SET_SELECTED_SQL, sql };
}

export function setUpdateSqlFromHistory({ updateSql }) {
  return { type: SET_UPDATE_SQL_FROM_HISTORY, updateSql };
}

export function setIsMultiQueryRunning({ running }) {
  return { type: SET_IS_MULTI_QUERY_RUNNING, running };
}

const defaultContext = new Immutable.List();
export function setQueryContext({ context }) {
  return { type: SET_QUERY_CONTEXT, context: context || defaultContext };
}

export function setQueryStatuses({ statuses = [] }) {
  return { type: SET_QUERY_STATUSES, statuses };
}

export function setQueryTabNumber({ tabNumber = 0 }) {
  return { type: SET_QUERY_TAB_NUMBER, tabNumber };
}

export function setQuerySelections({ selections = [] }) {
  return { type: SET_QUERY_SELECTIONS, selections };
}

export function setQueryFilter({ term = "" }) {
  return { type: SET_QUERY_FILTER, term };
}

export function resetNewQuery(viewId) {
  return { type: RESET_NEW_QUERY, viewId };
}

export function resetQueryState(obj = {}) {
  return { type: RESET_QUERY_STATE, ...obj };
}

export const UPDATE_COLUMN_FILTER = "UPDATE_COLUMN_FILTER";
export const updateColumnFilter = (columnFilter) => ({
  type: UPDATE_COLUMN_FILTER,
  columnFilter,
});

export const focusSqlEditor = () => ({ type: FOCUS_EDITOR });

export const datasetMetadataActions =
  actionUtils.generateRequestActions("DATASET_METADATA");
export const startDatasetMetadataLoad = () => ({
  type: datasetMetadataActions.start,
});
export const completeDatasetMetadataLoad = () => ({
  type: datasetMetadataActions.success,
});
