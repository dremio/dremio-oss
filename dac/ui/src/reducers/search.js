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
import Immutable from 'immutable';

import * as ActionTypes from 'actions/search';

const initialState = Immutable.fromJS({
  searchDatasets: [],
  hideRequest: true,
  searchText: null // string
});

export default function search(state = initialState, action) {
  switch (action.type) {
  case ActionTypes.SEARCH_SUCCESS:
    return state.set('searchDatasets', Immutable.List(action.payload.get('result').toArray()));
  // This is a synchronous action.
  case ActionTypes.HIDE_BAR_REQUEST:
    return state.set('hideRequest', true);

  case ActionTypes.NEW_SEARCH_REQUEST:
    return state.set('searchText', action.text);
  case ActionTypes.NEW_SEARCH_REQUEST_CLEANUP:
    return state.delete('searchText');
  default:
    return state;
  }
}
