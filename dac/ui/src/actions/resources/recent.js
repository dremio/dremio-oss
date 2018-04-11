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
import { CALL_API } from 'redux-api-middleware';

import { CALL_MOCK_API } from 'mockApi';
import { API_URL } from 'constants/Api';

const RECENT_DATASETS_DEFAULT_LIMIT = 20;

export const LOAD_RECENT_DATASETS_START = 'LOAD_RECENT_DATASETS_START';
export const LOAD_RECENT_DATASETS_SUCCESS = 'LOAD_RECENT_DATASETS_SUCCESS';
export const LOAD_RECENT_DATASETS_FAILURE = 'LOAD_RECENT_DATASETS_FAILURE';

function fetchRecentDatasets(limit) {
  const items = [];
  items.push('limit=' + (limit || RECENT_DATASETS_DEFAULT_LIMIT));

  const query = items.length && items.reduce((prevItem, curItem) => {
    return prevItem + (curItem ? '&' + curItem : '');
  }, '?') || '';

  return {
    [CALL_MOCK_API]: {
      types: [LOAD_RECENT_DATASETS_START, LOAD_RECENT_DATASETS_SUCCESS, LOAD_RECENT_DATASETS_FAILURE],
      method: 'GET',
      endpoint: API_URL + '/recent' + query,
      mockResponse: {
        responseJSON: require('../../mocks/recent.json')
      }
    }
  };
}

export function loadRecentDatasets(limit) {
  return (dispatch) => {
    return dispatch(fetchRecentDatasets(limit));
  };
}

export const LOAD_FILTERED_RECENT_DATASETS_START = 'LOAD_FILTERED_RECENT_DATASETS_START';
export const LOAD_FILTERED_RECENT_DATASETS_SUCCESS = 'LOAD_FILTERED_RECENT_DATASETS_SUCCESS';
export const LOAD_FILTERED_RECENT_DATASETS_FAILURE = 'LOAD_FILTERED_RECENT_DATASETS_FAILURE';

function fetchFilteredRecentDatasets(pattern) {
  return {
    [CALL_API]: {
      types: [
        LOAD_FILTERED_RECENT_DATASETS_START,
        LOAD_FILTERED_RECENT_DATASETS_SUCCESS,
        LOAD_FILTERED_RECENT_DATASETS_FAILURE
      ],
      method: 'GET',
      endpoint: API_URL + '/search?q=' + pattern + '&type=recentdatasets'
    }
  };
}

export function searchRecentDatasets(pattern, limit) {
  return (dispatch) => {
    return dispatch(fetchFilteredRecentDatasets(pattern, limit));
  };
}

export const CLEAR_HISTORY_START = 'CLEAR_HISTORY_START';
export const CLEAR_HISTORY_SUCCESS = 'CLEAR_HISTORY_SUCCESS';
export const CLEAR_HISTORY_FAILURE = 'CLEAR_HISTORY_FAILURE';

function clearHistoryRequest() {
  return {
    [CALL_API]: {
      types: [
        CLEAR_HISTORY_START,
        CLEAR_HISTORY_SUCCESS,
        CLEAR_HISTORY_FAILURE
      ],
      method: 'DELETE',
      endpoint: API_URL + '/recent'
    }
  };
}

export function clearHistory() {
  return (dispatch) => {
    return dispatch(clearHistoryRequest());
  };
}

export const REMOVE_HISTORY_START = 'REMOVE_HISTORY_START';
export const REMOVE_HISTORY_SUCCESS = 'REMOVE_HISTORY_SUCCESS';
export const REMOVE_HISTORY_FAILURE = 'REMOVE_HISTORY_FAILURE';

function removeFromHistoryRequest(id) {
  return {
    [CALL_API]: {
      types: [
        REMOVE_HISTORY_START,
        REMOVE_HISTORY_SUCCESS,
        REMOVE_HISTORY_FAILURE
      ],
      method: 'POST',
      body: JSON.stringify({
        id
      }),
      endpoint: API_URL + '/recent'
    }
  };
}

export function removeFromHistory(id) {
  return (dispatch) => {
    return dispatch(removeFromHistoryRequest(id));
  };
}
