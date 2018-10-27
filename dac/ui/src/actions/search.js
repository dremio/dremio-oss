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
import { API_URL_V2 } from 'constants/Api';
import searchSchema from 'schemas/dataset';
import { arrayOf } from 'normalizr';
import schemaUtils from 'utils/apiUtils/schemaUtils';

export const SEARCH_STARTED = 'SEARCH_STARTED';
export const SEARCH_SUCCESS = 'SEARCH_SUCCESS';
export const SEARCH_FAILURE = 'SEARCH_FAILURE';
export const HIDE_BAR_REQUEST = 'HIDE_BAR_REQUEST';
export const NEW_SEARCH_REQUEST = 'NEW_SEARCH_REQUEST'; /* is used to force a new search.
  Use case: user clicks on tag. We start search by clicked tag */
export const NEW_SEARCH_REQUEST_CLEANUP = 'NEW_SEARCH_REQUEST_CLEANUP'; // will be fired to clear redux store state

function fetchSearchData(text) {
  const meta = {viewId: 'searchDatasets'};
  return {
    [CALL_API]: {
      types: [
        { type: SEARCH_STARTED, meta },
        schemaUtils.getSuccessActionTypeWithSchema(SEARCH_SUCCESS, {datasets: arrayOf(searchSchema)}, meta),
        { type: SEARCH_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/datasets/search?filter=${encodeURIComponent(text)}`
    }
  };
}

export function loadSearchData(text) {
  return (dispatch) => {
    return dispatch(fetchSearchData(text));
  };
}

export const startSearch = dispatch => text => {
  dispatch({
    type: NEW_SEARCH_REQUEST,
    text
  });

  //schedule a redux store state cleanup
  setTimeout(() => {
    dispatch({
      type: NEW_SEARCH_REQUEST_CLEANUP
    });
  }, 1000);
};

export function hideBarRequest() {
  return {
    type: HIDE_BAR_REQUEST
  };
}
