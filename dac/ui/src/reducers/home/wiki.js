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
import { combineReducers } from 'redux';
import { wikiActions, WIKI_SAVED } from '@app/actions/home';
import { isLoading, isLoaded, errorMessageReducer, errorMessageId } from '@app/reducers/reducerFactories';

const enitityIdReducer = (state = null, { type, entityId }) => {
  switch (type) {
  case wikiActions.start:
  case wikiActions.success:
  case WIKI_SAVED:
    return entityId;
  case wikiActions.failure:
  default:
    return state;
  }
};

const wikiValue = (state = {
  text: '',
  version: null
}, { type, text = '', version = null }) => {
  switch (type) {
  case wikiActions.start:
  case wikiActions.failure:
    return null;
  case wikiActions.success:
  case WIKI_SAVED:
    return { text, version };
  default:
    return state;
  }
};

export default combineReducers({
  enitityId: enitityIdReducer,
  isLoading: isLoading(wikiActions),
  isLoaded: isLoaded(wikiActions),
  wikiValue,
  errorMessage: errorMessageReducer(wikiActions),
  errorId: errorMessageId(wikiActions)
});

export const isWikiLoaded = (state, entityId) => state.enitityId === entityId && state.isLoaded;
export const isWikiLoading = (state, entityId) => state.enitityId === entityId && state.isLoading;
export const getWiki = (state, entityId) => isWikiLoaded(state, entityId) ? state.wikiValue.text : null;
export const getWikiVersion = (state, entityId) => isWikiLoaded(state, entityId) ? state.wikiValue.version : null;
export const getErrorInfo = (state, enitityId) => state.errorMessage ? ({
  message: state.errorMessage,
  id: state.errorMessageId
}) : null;

