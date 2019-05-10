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

import { SET_CURRENT_SQL, RESET_NEW_QUERY, SET_QUERY_CONTEXT, FOCUS_EDITOR,
  datasetMetadataActions } from 'actions/explore/view';
import { RUN_TABLE_TRANSFORM_SUCCESS } from 'actions/explore/dataset/common';
import { LOAD_EXPLORE_ENTITIES_SUCCESS } from 'actions/explore/dataset/get';
import { RUN_DATASET_SUCCESS } from 'actions/explore/dataset/run';
import { isLoaded } from '@app/reducers/reducerFactories';
import { combineReducers } from 'redux';

export const EXPLORE_VIEW_ID = 'EXPLORE_VIEW_ID';
export const EXPLORE_TABLE_ID = 'EXPLORE_TABLE_ID';

// currentSql === null means sql has not been changed
const currentSql = (state = null, { type, sql }) => {
  switch (type) {
  case SET_CURRENT_SQL:
    return sql === undefined ? null : sql;
  case RESET_NEW_QUERY:
    return null;
  default:
    return state;
  }
};

const queryContext = (state = Immutable.List(), { type, context }) => {
  switch (type) {
  case SET_QUERY_CONTEXT: {
    return context;
  }
  default:
    return state;
  }
};

const isPreviewMode = (state = true, { type }) => {
  switch (type) {
  case RUN_DATASET_SUCCESS:
    return false;
  case LOAD_EXPLORE_ENTITIES_SUCCESS:
  case RUN_TABLE_TRANSFORM_SUCCESS:
    return true;
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

export default combineReducers({
  queryContext,
  currentSql,
  isPreviewMode,
  sqlEditorFocusKey,
  // DX-14650 as of now this filed is used to indicate whether or not disable headers in the table
  // on explore page. Metadata here includes sql, query context, table columns, history. Data is not
  // included.
  isDatasetMetadataLoaded: isLoaded(datasetMetadataActions)
});
