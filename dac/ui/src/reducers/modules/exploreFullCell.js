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
import { CLEAR_FULL_CELL_VALUE, LOAD_FULL_CELL_VALUE_SUCCESS } from 'actions/explore/dataset/data';
import { getModuleState } from '@app/reducers';

// describes a state which is used in case when user clicks '...' button explore page table.
// as this functionality is used not only on explore page, but on adding a fail for example,
// we need to put it in a separate module

export const moduleKey = 'exploreFullCell';

const initialState = Immutable.fromJS({
  fullCell: {
    value: '',
    isInProgress: false,
    isFailed: false
  }
});

export default function exploreFullCell(state = initialState, action) {
  switch (action.type) {

  case CLEAR_FULL_CELL_VALUE:
    return state.setIn(['fullCell', 'value'], '');

  case LOAD_FULL_CELL_VALUE_SUCCESS: {
    const fullValue = Immutable.fromJS(action.payload);
    return state.setIn(['fullCell', 'value'], fullValue);
  }
  default:
    return state;
  }
}

export const getCell = state => getModuleState(state, moduleKey).get('fullCell');
