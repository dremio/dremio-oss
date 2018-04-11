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
import { shallow } from 'enzyme';
import Immutable from 'immutable';

import AccelerationContent from './AccelerationContent';

describe('AccelerationContent', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      jobDetails: Immutable.fromJS({
        jobId: {
          id: 'jobid'
        },
        requestType: 'CREATE_PREPARE',
        state: 'COMPLETED',
        endTime: 123546,
        startTime: 0,
        user: 'dremio',
        queryType: 'UI_RUN',
        stats: {},
        sql: 'SELECT * FROM',
        attemptDetails: [
          {
            reason: 'schema Learning',
            profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
          },
          {
            reason: 'Insufficient memory',
            profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
          }
        ],
        parentsList: [
          {
            datasetPathList: ['Prod-Sample', 'ds1'],
            type: 'VIRTUAL_DATASET'
          }
        ]
      })
    };
    commonProps = {
      ...minimalProps,
      jobDetails: minimalProps.jobDetails.set({
        acceleration: Immutable.fromJS({
          reflectionRelationships: []
        })
      })
    };

    wrapper = shallow(<AccelerationContent {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    expect(shallow(<AccelerationContent {...minimalProps}/>)).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    expect(wrapper).to.have.length(1);
  });
});
