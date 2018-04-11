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

import * as ActionTypes from 'actions/home';
import homeMapper from 'utils/mappers/homeMapper';

const initialState = Immutable.fromJS({
  datasets: [],
  isInProgress: false,
  isFailed: false
});

export default function recentDatasets(state = initialState, action) {

  switch (action.type) {

  case ActionTypes.LOAD_RECENT_DATASETS_START:
    return Immutable.fromJS({
      datasets: state.get('datasets'),
      isInProgress: true,
      isFailed: false
    });

  case ActionTypes.LOAD_RECENT_DATASETS_SUCCESS:
    return Immutable.fromJS({
      datasets: homeMapper.mapRecentDatasets(state.get('datasets').toJS(), action.payload.summaries),
      isInProgress: false,
      isFailed: false
    });

  case ActionTypes.LOAD_RECENT_DATASETS_FAILURE:
    return Immutable.fromJS({
      datasets: [],
      isInProgress: false,
      isFailed: true
    });

  case ActionTypes.LOAD_FILTERED_RECENT_DATASETS_START:
    return state.set('datasets', new Immutable.List()).set('isInProgress', true).set('isFailed', false);

  case ActionTypes.LOAD_FILTERED_RECENT_DATASETS_SUCCESS:
    return state.set('datasets', Immutable.fromJS(homeMapper.mapRecentDatasets(action.payload.recent)))
                .set('isInProgress', false).set('isFailed', false);

  case ActionTypes.LOAD_FILTERED_RECENT_DATASETS_FAILURE:
    return state.set('datasets', new Immutable.List())
                .set('isInProgress', false).set('isFailed', true);

  default:
    return state;
  }
}
