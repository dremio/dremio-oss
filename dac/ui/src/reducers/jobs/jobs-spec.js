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

import {
  UPDATE_JOB_STATE
} from 'actions/jobs/jobs';

import jobsReducer from './jobs';

describe('jobs reducer', () => {
  const initialState =  Immutable.fromJS({
    jobDetails: {
      outputRecords: 10
    },
    jobs: [
      {
        id: 'a-b-c-d',
        datasetVersion: '123',
        state: 'RUNNING',
        datasetPathList: ['myspace', 'foo'],
        datasetType: 'VIRTUAL_DATASET'
      }
    ]
  });

  it('returns unaltered state by default', () => {
    const result = jobsReducer(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  describe('UPDATE_JOB_STATE', () => {
    it('should job state in jobs list, but keep datasetPathList and datasetType', () => {
      const result = jobsReducer(initialState, {
        type: UPDATE_JOB_STATE,
        jobId: 'a-b-c-d',
        payload: {
          id: 'a-b-c-d',
          state: 'COMPLETED',
          isComplete: false
        }
      });
      expect(result.getIn(['jobs', 0, 'state'])).to.eql('COMPLETED');
      expect(result.getIn(['jobs', 0, 'isComplete'])).to.eql(false);
      expect(result.getIn(['jobs', 0, 'datasetPathList'])).to.eql(initialState.getIn(['jobs', 0, 'datasetPathList']));
      expect(result.getIn(['jobs', 0, 'datasetType'])).to.eql(initialState.getIn(['jobs', 0, 'datasetType']));
    });
  });
});
