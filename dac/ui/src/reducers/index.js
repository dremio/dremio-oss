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
import { routerReducer } from 'react-router-redux';
import { combineReducers } from 'redux';
import { reducer as formReducer } from 'redux-form';
import { get } from 'lodash';

import { LOGOUT_USER_START, NO_USERS_ERROR } from 'actions/account';
import developmentOptions from 'dyn-load/reducers/developmentOptions';
import account from '@inject/reducers/account';
import admin from 'dyn-load/reducers/admin';
import { getExploreState } from '@app/selectors/explore';
import { log } from '@app/utils/logger';

import search from './search';

import home from './home/home';
import ui from './ui/ui';

import jobs from './jobs/index';
import modals from './modals/index';

import serverStatus from './serverStatus';

import resources from './resources';
import notification from './notification';
import confirmation from './confirmation';
import prodError, { getError, getErrorId } from './prodError';
import modulesState, { getData, isInitialized } from './modulesState';

const appReducers = combineReducers({
  resources,
  ui,
  home,
  account,
  jobs,
  modals,
  admin,
  search,
  developmentOptions,
  notification,
  serverStatus,
  form: formReducer,
  routing: routerReducer,
  confirmation,
  appError: prodError,
  modulesState
});

const actionCancelGroups = {};

const cancelAction = reducer => (state, action) => {
  const {
    actionGroup, // group name
    abortController, // if abortController is provided, the action is abortable/cancellable
    startTime
  } = get(action, 'meta.abortInfo', {});

  if (actionGroup) {
    log('==> ActionCancelGroup: ', actionGroup);
    const group = actionCancelGroups[actionGroup];
    const groupAbortController = get(group, 'abortController');
    const { message: payloadMessage } = get(action, 'payload') || {};

    // to ignore an aborted request the proper way is to catch promise error:
    // fetch....then(....)
    // .catch(error => {
    //         if (error.name === 'AbortError') return; // expected, this is the abort, so just return
    //         throw error; // must be some other error, handle however you normally would
    //       }
    // but since the fetch is buried deep in layers of 3rd party middleware libraries,
    // we check the abort message here with 'Aborted' for IE and Edge and longer message fro Chrome family
    if (payloadMessage === 'The user aborted a request.' || payloadMessage === 'Aborted') {
      return state;
    }

    if (groupAbortController) { // cancellation is needed for previous action
      log(`==> Action ${JSON.stringify(group.action, null, 2)} was cancelled by action ${JSON.stringify(action, null, 2)}`);
      groupAbortController.abort(); // abort fetch request which throws 'AbortError'
    }
    if (abortController) { // new action is abortable -> update saved cancel group
      actionCancelGroups[actionGroup] = {
        action,  // used here only for logging and debugging
        abortController,
        startTime
      };
    } else {
      actionCancelGroups[actionGroup] = null; // remove previous abortable action from the groups
    }
  }

  return reducer(state, action);
};


export default cancelAction(function rootReducer(state, action) {
  let nextState = state;
  // we only needed to keep the user info around long enough to prep the /logout
  // so once we get LOGOUT_USER_START we are safe to clear things out without waiting any more
  // (we also don't want to do anything differently on failure)
  // also need to clear out and socket close for NO_USERS_ERROR
  if (action.type === LOGOUT_USER_START || action.type === NO_USERS_ERROR) {
    // reset the app state (but keep routing)
    // (this needs to happen before other reducers so that they go back to their initial state - thus why this is in this file)
    const { routing } = state || {};
    nextState = { routing };
  }

  const result = appReducers(nextState, action);
  return result;
});

export const getIsExplorePreviewMode = state => {
  const exploreState = getExploreState(state);
  return exploreState ? exploreState.view.isPreviewMode : false;
};
export const getIsDatasetMetadataLoaded = state => {
  const exploreState = getExploreState(state);
  return exploreState ? exploreState.view.isDatasetMetadataLoaded : false;
};
export const getUser = state => state.account.get('user');
export const isModuleInitialized = (state, moduleKey) => isInitialized(state.modulesState, moduleKey);
export const getModuleState = (state, moduleKey) => getData(state.modulesState, moduleKey);
export const getAppError = state => getError(state.appError);
export const getAppErrorId = state => getErrorId(state.appError);
