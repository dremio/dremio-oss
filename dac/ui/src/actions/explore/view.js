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

export const SET_PREVIOUS_MULTI_SQL = "SET_PREVIOUS_MULTI_SQL";
export const SET_PREVIOUS_AND_CURRENT_SQL = "SET_PREVIOUS_AND_CURRENT_SQL";
export const SET_SELECTED_SQL = "SET_SELECTED_SQL";
export const SET_CUSTOM_DEFAULT_SQL = "SET_CUSTOM_DEFAULT_SQL";
export const SET_UPDATE_SQL_FROM_HISTORY = "SET_UPDATE_SQL_FROM_HISTORY";
export const SET_IS_MULTI_QUERY_RUNNING = "SET_IS_MULTI_QUERY_RUNNING";
export const SET_QUERY_CONTEXT = "SET_QUERY_CONTEXT";
export const SET_QUERY_STATUSES = "SET_QUERY_STATUSES";
export const SET_QUERY_TAB_NUMBER = "SET_QUERY_TAB_NUMBER";
export const SET_QUERY_SELECTIONS = "SET_QUERY_SELECTIONS";
export const SET_QUERY_FILTER = "SET_QUERY_FILTER";
export const SET_ACTION_STATE = "SET_ACTION_STATE";
export const RESET_NEW_QUERY = "RESET_NEW_QUERY";
export const RESET_QUERY_STATE = "RESET_QUERY_STATE";
export const FOCUS_EDITOR = "FOCUS_SQL_EDITOR";
export const RESET_TABLE_STATE = "RESET_TABLE_STATE";

/**
 * Used to initialize / reset the contents of the SQL runner
 */
export const SET_CURRENT_SQL = "SET_CURRENT_SQL";

/**
 * Indicates that the contents of the SQL runner have been modified after being initialized
 */
export const MODIFY_CURRENT_SQL = "MODIFY_CURRENT_SQL";

export function setCurrentSql({ sql }) {
  return { type: SET_CURRENT_SQL, sql };
}

export function modifyCurrentSql({ sql }) {
  return { type: MODIFY_CURRENT_SQL, sql };
}

export function setPreviousMultiSql({ sql, tabId }) {
  return { type: SET_PREVIOUS_MULTI_SQL, sql, tabId };
}

export function setPreviousAndCurrentSql({ sql, tabId }) {
  return { type: SET_PREVIOUS_AND_CURRENT_SQL, sql, tabId };
}

export function setSelectedSql({ sql }) {
  return { type: SET_SELECTED_SQL, sql };
}

export function setCustomDefaultSql({ sql }) {
  return { type: SET_CUSTOM_DEFAULT_SQL, sql };
}

export function setUpdateSqlFromHistory({ updateSql }) {
  return { type: SET_UPDATE_SQL_FROM_HISTORY, updateSql };
}

export function setIsMultiQueryRunning({ running, tabId }) {
  return { type: SET_IS_MULTI_QUERY_RUNNING, running, tabId };
}

const defaultContext = new Immutable.List();
export function setQueryContext({ context }) {
  return { type: SET_QUERY_CONTEXT, context: context || defaultContext };
}

export function setQueryStatuses({ statuses = [], tabId = "" }) {
  return { type: SET_QUERY_STATUSES, statuses, tabId };
}

export function setQueryTabNumber({ tabNumber = 0 }) {
  return { type: SET_QUERY_TAB_NUMBER, tabNumber };
}

export function setQuerySelections({ selections = [], tabId }) {
  return { type: SET_QUERY_SELECTIONS, selections, tabId };
}

export function setQueryFilter({ term = "" }) {
  return { type: SET_QUERY_FILTER, term };
}

export function setActionState({ actionState }) {
  return { type: SET_ACTION_STATE, actionState };
}

export function resetNewQuery(viewId) {
  return { type: RESET_NEW_QUERY, viewId };
}

export function resetQueryState(obj = {}) {
  return { type: RESET_QUERY_STATE, ...obj };
}

export function resetTableState(obj = {}) {
  return { type: RESET_TABLE_STATE, ...obj };
}

export const UPDATE_COLUMN_FILTER = "UPDATE_COLUMN_FILTER";
export const updateColumnFilter = (columnFilter, datasetVersion) => ({
  type: UPDATE_COLUMN_FILTER,
  columnFilter,
  datasetVersion,
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
