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

import { LOGOUT_USER_START, NO_USERS_ERROR } from 'actions/account';
import developmentOptions from 'dyn-load/reducers/developmentOptions';
import admin from 'dyn-load/reducers/admin';
import { getExploreState } from '@app/selectors/explore';

import search from './search';

import home from './home/home';
import ui from './ui/ui';
import account from './account';

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

export default function rootReducer(state, action) {
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
}

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
