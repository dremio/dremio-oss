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

import { UPDATE_HISTORY_WITH_JOB_STATE } from 'actions/explore/history';

import historyItemReducer from './historyItem';

describe('historyItem entity reducer', () => {
  const initialState =  Immutable.fromJS({
    historyItem: {
      '123': {
        version: '123', state: 'COMPLETED'
      },
      '456': {
        version: '456', state: 'RUNNING'
      }
    }
  });

  it('returns unaltered state by default', () => {
    const result = historyItemReducer(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  describe('UPDATE_HISTORY_WITH_JOB_STATE', () => {
    it('should update non-tip via history of tip dataset', () => {

      const result = historyItemReducer(initialState, {
        type: UPDATE_HISTORY_WITH_JOB_STATE,
        meta: {
          dataset: Immutable.fromJS({
            tipVersion: '123',
            datasetVersion: '456'
          }),
          jobState: 'COMPLETED'
        }
      });
      expect(result.getIn(['historyItem', '123', 'state'])).to.eql('COMPLETED');
    });
  });
});
