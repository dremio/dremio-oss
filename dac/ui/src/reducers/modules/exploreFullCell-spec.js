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

import { CLEAR_FULL_CELL_VALUE, LOAD_FULL_CELL_VALUE_SUCCESS } from 'actions/explore/dataset/data';
import exploreFullCell from './exploreFullCell';

describe('explore dataset reducer', () => {

  const initialState =  Immutable.fromJS({
    fullCell: {
      value: 'val',
      isInProgress: false,
      isFailed: false
    }
  });

  it('returns unaltered state by default', () => {
    const result = exploreFullCell(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  it('should clear cell value', () => {
    const result = exploreFullCell(initialState, {
      type: CLEAR_FULL_CELL_VALUE
    });
    expect(result.getIn(['fullCell', 'value'])).to.be.empty;
  });

  it('should set full value of cell', () => {
    const result = exploreFullCell(initialState, {
      type: LOAD_FULL_CELL_VALUE_SUCCESS,
      payload: 'lallalallallalalla'
    });
    expect(result.getIn(['fullCell', 'value'])).to.be.equal('lallalallallalalla');
  });
});
