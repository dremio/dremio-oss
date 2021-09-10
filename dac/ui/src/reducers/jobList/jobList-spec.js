/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
  SET_JOB_LIST_CLUSTER_TYPE
} from 'actions/joblist/jobList';

import jobsListReducer from './jobList';

describe('jobs reducer', () => {
  const initialState = Immutable.fromJS({
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
    const result = jobsListReducer(initialState, { type: 'bla' });
    expect(result).to.equal(initialState);
  });

  describe('SET_JOB_LIST_CLUSTER_TYPE', () => {
    it('should update cluster type', () => {
      const result = jobsListReducer(initialState, {
        type: SET_JOB_LIST_CLUSTER_TYPE,
        payload: {
          clusterType: 'yarn',
          isSupport: true
        }
      });
      expect(result.get('clusterType')).to.eql('yarn');
      expect(result.get('isSupport')).to.eql(true);
    });
  });
});
