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

import * as ActionTypes from 'actions/explore/sqlActions';
import gridTableMapper from 'utils/mappers/gridTableMapper';

const initialState = Immutable.fromJS({
  newDateset: {
    dataset: {},
    isInProgress: false,
    isFailed: false
  },
  gridHelpData: {}
});

export default function grid(state = initialState, action) {
  switch (action.type) {

  case ActionTypes.CREATE_DATASET_FROM_EXISTING_START:
  case ActionTypes.CREATE_DATASET_START:
    return state.setIn(['newDateset', 'isInProgress'], true).setIn(['newDateset', 'isFailed'], false);

  case ActionTypes.CREATE_DATASET_FROM_EXISTING_SUCCESS:
  case ActionTypes.CREATE_DATASET_SUCCESS: {
    const newDataset = Immutable.fromJS(action.payload);
    return state.setIn(['newDateset', 'dataset'], newDataset)
                .setIn(['newDateset', 'isInProgress'], false)
                .setIn(['newDateset', 'isFailed'], false);
  }
  case ActionTypes.CREATE_DATASET_FROM_EXISTING_FAILURE:
  case ActionTypes.CREATE_DATASET_FAILURE:
    return state.setIn(['newDateset', 'isInProgress'], false).setIn(['lookup', 'isFailed'], true);

  case ActionTypes.SQL_HELP_FUNC_SUCCESS:
    return state.set('gridHelpData', Immutable.fromJS({
      items: gridTableMapper.mapHelpFunctions(action.meta.sqlFuncs),
      isInProgress: false,
      isFailed: false
    }));

  default:
    return state;
  }
}
