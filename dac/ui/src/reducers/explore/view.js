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
  MODIFY_CURRENT_SQL,
  RESET_NEW_QUERY,
  SET_QUERY_CONTEXT,
  FOCUS_EDITOR,
  datasetMetadataActions,
  SET_QUERY_STATUSES,
  SET_QUERY_FILTER,
  SET_QUERY_SELECTIONS,
  SET_PREVIOUS_MULTI_SQL,
  SET_SELECTED_SQL,
  SET_CUSTOM_DEFAULT_SQL,
  SET_IS_MULTI_QUERY_RUNNING,
  SET_QUERY_TAB_NUMBER,
  SET_ACTION_STATE,
  RESET_QUERY_STATE,
  SET_PREVIOUS_AND_CURRENT_SQL,
  SET_UPDATE_SQL_FROM_HISTORY,
  RESET_TABLE_STATE,
  WAIT_FOR_JOB_RESULTS,
  SET_EXPLORE_TABLE_LOADING,
  SET_EDITOR_CONTENTS,
} from "@app/actions/explore/view";
import { isLoaded } from "@app/reducers/reducerFactories";
import { combineReducers } from "redux";
import {
  SELECT_ACTIVE_SCRIPT,
  CLEAR_SCRIPT_STATE,
  REPLACE_SCRIPT_CONTENTS,
  REMOVE_TAB_VIEW,
  SET_TAB_VIEW,
} from "@app/actions/resources/scripts";

export const EXPLORE_VIEW_ID = "EXPLORE_VIEW_ID";
export const EXPLORE_TABLE_ID = "EXPLORE_TABLE_ID";

const hasTabId = (action) => {
  // May consolidate later, don't need meta.tabId and tabId
  const tabId = action?.tabId || action?.meta?.tabId;
  return !!tabId;
};

// currentSql === null means sql has not been changed
const currentSql = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, sql, exclude } = action;

  switch (type) {
    case SET_CURRENT_SQL:
    case MODIFY_CURRENT_SQL:
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

const previousMultiSql = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, sql } = action;

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

const selectedSql = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, sql } = action;

  switch (type) {
    case SET_SELECTED_SQL:
      return sql === undefined ? null : sql;
    case RESET_QUERY_STATE:
      return "";
    default:
      return state;
  }
};

const customDefaultSql = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, sql } = action;

  switch (type) {
    case SET_CUSTOM_DEFAULT_SQL:
      return sql === undefined ? null : sql;
    case RESET_QUERY_STATE:
      return null;
    default:
      return state;
  }
};

const updateSqlFromHistory = (state = false, action) => {
  if (hasTabId(action)) return state;

  const { type, updateSql } = action;

  switch (type) {
    case SET_UPDATE_SQL_FROM_HISTORY:
      return updateSql;
    default:
      return state;
  }
};

const isMultiQueryRunning = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, running } = action;

  switch (type) {
    case SET_IS_MULTI_QUERY_RUNNING:
      return running;
    default:
      return state;
  }
};

const queryContext = (state = Immutable.List(), action) => {
  if (hasTabId(action)) return state;

  const { type, context } = action;

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

const queryStatuses = (state = [], action) => {
  if (hasTabId(action)) return state;

  const { type, statuses } = action;

  switch (type) {
    case SET_QUERY_STATUSES:
      return statuses;
    case RESET_QUERY_STATE:
    case RESET_TABLE_STATE:
      return [];
    default:
      return state;
  }
};

const queryTabNumber = (state = 0, action) => {
  if (hasTabId(action)) return state;

  const { type, tabNumber } = action;

  switch (type) {
    case SET_QUERY_TAB_NUMBER:
      return tabNumber;
    case RESET_QUERY_STATE:
    case RESET_TABLE_STATE:
      return 0;
    default:
      return state;
  }
};

const querySelections = (state = [], action) => {
  if (hasTabId(action)) return state;

  const { type, selections } = action;

  switch (type) {
    case SET_QUERY_SELECTIONS:
      return selections;
    case RESET_QUERY_STATE:
    case RESET_TABLE_STATE:
      return [];
    default:
      return state;
  }
};

const queryFilter = (state = "", action) => {
  if (hasTabId(action)) return state;

  const { type, term } = action;

  switch (type) {
    case SET_QUERY_FILTER: {
      return term;
    }

    // Maybe reset on RESET_QUERY_STATE
    default:
      return state;
  }
};

const waitingForJobResults = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, jobId } = action;

  switch (type) {
    case WAIT_FOR_JOB_RESULTS:
      return jobId;
    case MODIFY_CURRENT_SQL:
      return null; // reset the loading flag if script changes
    default:
      return state;
  }
};

const isExploreTableLoading = (state = false, action) => {
  if (hasTabId(action)) return state;

  const { type, isLoading } = action;

  switch (type) {
    case SET_EXPLORE_TABLE_LOADING:
      return isLoading;
    case SET_TAB_VIEW:
      return false; // reset the loading flag if script changes
    default:
      return state;
  }
};

const editorContents = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, content } = action;

  switch (type) {
    case SET_EDITOR_CONTENTS:
      return content;
    default:
      return state;
  }
};

const sqlEditorFocusKey = (state = 0, action) => {
  if (hasTabId(action)) return state;

  const { type } = action;

  switch (type) {
    case FOCUS_EDITOR:
      return new Date().getTime(); // todo replace with a key provider as reducer should be a pure function
    default:
      return state;
  }
};

const activeScript = (state = {}, action) => {
  if (hasTabId(action)) return state;

  const { type, script } = action;

  switch (type) {
    case SELECT_ACTIVE_SCRIPT:
      return script.script;
    case REPLACE_SCRIPT_CONTENTS: {
      if (action.script.id !== state.id) {
        return state;
      }
      return action.script;
    }
    case CLEAR_SCRIPT_STATE:
    case RESET_QUERY_STATE: {
      return {};
    }
    case REMOVE_TAB_VIEW: {
      const { scriptId } = action;
      if (scriptId === state.id) {
        return {};
      }
      return state;
    }
    default:
      return state;
  }
};

const actionState = (state = null, action) => {
  if (hasTabId(action)) return state;

  const { type, actionState } = action;

  switch (type) {
    case SET_ACTION_STATE:
      return actionState;
    default:
      return state;
  }
};

export default combineReducers({
  queryContext,
  currentSql,
  previousMultiSql,
  selectedSql,
  customDefaultSql,
  updateSqlFromHistory,
  isMultiQueryRunning,
  sqlEditorFocusKey,
  queryStatuses,
  queryTabNumber,
  querySelections,
  queryFilter,
  actionState,
  // DX-14650 as of now this filed is used to indicate whether or not disable headers in the table
  // on explore page. Metadata here includes sql, query context, table columns, history. Data is not
  // included.
  isDatasetMetadataLoaded: isLoaded(datasetMetadataActions),
  activeScript,
  waitingForJobResults,
  isExploreTableLoading,
  editorContents,
});
