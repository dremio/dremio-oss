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

import * as ActionTypes from 'actions/explore/ui';
import * as QlikActions from 'actions/qlik';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import {hashHeightTopSplitter} from 'constants/explorePage/heightTopSplitter';

function getInitialState() {
  return Immutable.fromJS({
    sqlState: localStorageUtils.getDefaultSqlState(),
    sqlSize: hashHeightTopSplitter.getDefaultSqlHeight(),
    isResizeInProgress: false,
    isUnsavedChangesModalShowing: false,
    qlikDialogVisible: false,
    qlikShowDialogDataset: null,
    qlikInProgress: false,
    qlikError: null,
    qlikAppCreationSuccess: false,
    qlikAppInfo: null
  });
}

function updateExploreSql(state, sqlState) {
  if (localStorageUtils) {
    localStorageUtils.setDefaultSqlState(sqlState);
  }
  return state.set('sqlState', sqlState);
}

export default function grid(state = getInitialState(), action) {
  switch (action.type) {

  case ActionTypes.TOGGLE_EXPLORE_SQL: {
    return updateExploreSql(state, !state.get('sqlState'));
  }
  case ActionTypes.COLLAPSE_EXPLORE_SQL:
    // do not use here updateExploreSql, as this action is used to collapse editor by default
    // and this state should not be saved.
    return state.set('sqlState', false);
  case ActionTypes.EXPAND_EXPLORE_SQL: {
    return updateExploreSql(state, true);
  }
  case ActionTypes.SET_SQL_EDITOR_SIZE:
    localStorageUtils.setDefaultSqlHeight(action.size);
    return state.set('sqlSize', action.size);
  case ActionTypes.SHOW_QLIK_MODAL:
    return state.set('qlikDialogVisible', true).set('qlikShowDialogDataset', action.dataset)
      .set('qlikAppCreationSuccess', false);
  case ActionTypes.HIDE_QLIK_MODAL:
    return state.set('qlikDialogVisible', false).set('qlikShowDialogDataset', null);
  case ActionTypes.RESIZE_PROGRESS_STATE:
    return state.set('isResizeInProgress', action.state);
  case ActionTypes.SHOW_QLIK_ERROR:
    return state.set('qlikError', Immutable.Map(action.payload)).set('qlikInProgress', false);
  case ActionTypes.HIDE_QLIK_ERROR:
    return state.delete('qlikError');
  case ActionTypes.SHOW_QLIK_PROGRESS:
    return state.set('qlikInProgress', true);
  case ActionTypes.HIDE_QLIK_PROGRESS:
    return state.set('qlikInProgress', false);
  case QlikActions.QLIK_APP_CREATION_SUCCESS:
    return state.set('qlikInProgress', false).set('qlikAppCreationSuccess', true).set('qlikAppInfo', action.info);

  default:
    return state;
  }
}
