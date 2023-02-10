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

import {
  SET_CURRENT_SQL,
  RESET_NEW_QUERY,
  SET_QUERY_CONTEXT,
  FOCUS_EDITOR,
  datasetMetadataActions,
  SET_QUERY_STATUSES,
  SET_QUERY_FILTER,
  SET_QUERY_SELECTIONS,
  SET_PREVIOUS_MULTI_SQL,
  SET_SELECTED_SQL,
  SET_IS_MULTI_QUERY_RUNNING,
  SET_QUERY_TAB_NUMBER,
  RESET_QUERY_STATE,
  SET_PREVIOUS_AND_CURRENT_SQL,
  SET_UPDATE_SQL_FROM_HISTORY,
} from "actions/explore/view";
import { isLoaded } from "@app/reducers/reducerFactories";
import { combineReducers } from "redux";
import {
  SELECT_ACTIVE_SCRIPT,
  CLEAR_SCRIPT_STATE,
} from "@app/actions/resources/scripts";

export const EXPLORE_VIEW_ID = "EXPLORE_VIEW_ID";
export const EXPLORE_TABLE_ID = "EXPLORE_TABLE_ID";

// currentSql === null means sql has not been changed
const currentSql = (state = null, { type, sql, exclude }) => {
  switch (type) {
    case SET_CURRENT_SQL:
    case SET_PREVIOUS_AND_CURRENT_SQL:
      return sql === undefined ? null : sql;
    case RESET_NEW_QUERY:
      return null;
    case RESET_QUERY_STATE:
      return exclude && exclude.length && exclude.includes("currentSql")
        ? state
        : "";
    default:
      return state;
  }
};

const previousMultiSql = (state = null, { type, sql }) => {
  switch (type) {
    case SET_PREVIOUS_MULTI_SQL:
    case SET_PREVIOUS_AND_CURRENT_SQL:
      return sql === undefined ? null : sql;
    case RESET_QUERY_STATE:
      return "";
    default:
      return state;
  }
};

const selectedSql = (state = null, { type, sql }) => {
  switch (type) {
    case SET_SELECTED_SQL:
      return sql === undefined ? null : sql;
    case RESET_QUERY_STATE:
      return "";
    default:
      return state;
  }
};

const updateSqlFromHistory = (state = false, { type, updateSql }) => {
  switch (type) {
    case SET_UPDATE_SQL_FROM_HISTORY:
      return updateSql;
    default:
      return state;
  }
};

const isMultiQueryRunning = (state = null, { type, running }) => {
  switch (type) {
    case SET_IS_MULTI_QUERY_RUNNING:
      return running;
    default:
      return state;
  }
};

const queryContext = (state = Immutable.List(), { type, context }) => {
  switch (type) {
    case SET_QUERY_CONTEXT: {
      return context;
    }
    case RESET_QUERY_STATE: {
      return new Immutable.List();
    }
    default:
      return state;
  }
};

const queryStatuses = (state = [], { type, statuses }) => {
  switch (type) {
    case SET_QUERY_STATUSES:
      return statuses;
    case RESET_QUERY_STATE:
      return [];
    default:
      return state;
  }
};

const queryTabNumber = (state = 0, { type, tabNumber }) => {
  switch (type) {
    case SET_QUERY_TAB_NUMBER:
      return tabNumber;
    case RESET_QUERY_STATE:
      return 0;
    default:
      return state;
  }
};

const querySelections = (state = [], { type, selections }) => {
  switch (type) {
    case SET_QUERY_SELECTIONS:
      return selections;
    case RESET_QUERY_STATE:
      return [];
    default:
      return state;
  }
};

const queryFilter = (state = "", { type, term }) => {
  switch (type) {
    case SET_QUERY_FILTER: {
      return term;
    }
    default:
      return state;
  }
};

const sqlEditorFocusKey = (state = 0, { type }) => {
  switch (type) {
    case FOCUS_EDITOR:
      return new Date().getTime(); // todo replace with a key provider as reducer should be a pure function
    default:
      return state;
  }
};

const activeScript = (state = {}, { type, script }) => {
  switch (type) {
    case SELECT_ACTIVE_SCRIPT:
      return script.script;
    case CLEAR_SCRIPT_STATE:
    case RESET_QUERY_STATE: {
      return {};
    }
    default:
      return state;
  }
};

export default combineReducers({
  queryContext,
  currentSql,
  previousMultiSql,
  selectedSql,
  updateSqlFromHistory,
  isMultiQueryRunning,
  sqlEditorFocusKey,
  queryStatuses,
  queryTabNumber,
  querySelections,
  queryFilter,
  // DX-14650 as of now this filed is used to indicate whether or not disable headers in the table
  // on explore page. Metadata here includes sql, query context, table columns, history. Data is not
  // included.
  isDatasetMetadataLoaded: isLoaded(datasetMetadataActions),
  activeScript,
});
