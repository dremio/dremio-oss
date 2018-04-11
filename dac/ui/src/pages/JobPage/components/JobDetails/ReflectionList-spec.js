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

import ReflectionList from './ReflectionList';

describe('ReflectionList', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let context;
  beforeEach(() => {
    minimalProps = {
      jobDetails: Immutable.fromJS({
        startTime: 0
      }),
      reflections: [
        {
          'relationship': 'CONSIDERED',
          'materialization': {
            'id': '36c675f7-710e-4e01-998b-7664df71e355',
            'refreshChainStartTime': Date.now()
          },
          'dataset': {
            'id': '5506693e-a14d-468f-930f-73ab798fc9b8',
            'path': ['@dremio', 'zips'],
            'type': 'PHYSICAL_DATASET_HOME_FILE'
          },
          'accelerationSettings': {
            'method': 'FULL',
            'refreshPeriod': 33061210698000,
            'gracePeriod': 33061210698000
          },
          'reflection': {
            'partitionDistributionStrategy': 'CONSOLIDATED',
            'id': '064f0e51-5f20-45a6-a2c3-69e42ccf82af',
            'name': '',
            'latestMaterializationState': 'NEW',
            'state': 'ACTIVE',
            'type': 'RAW'
          }
        },
        {
          'relationship': 'CHOSEN',
          'materialization': {
            'id': 'd22aea08-3f12-48a6-b019-0d7d2e89a0d1',
            'refreshChainStartTime': Date.now()
          },
          'dataset': {
            'id': '5506693e-a14d-468f-930f-73ab798fc9b8',
            'path': ['@dremio', 'zips'],
            'type': 'PHYSICAL_DATASET_HOME_FILE'
          },
          'accelerationSettings': {
            'method': 'FULL',
            'refreshPeriod': 33061210698000,
            'gracePeriod': 33061210698000
          },
          'reflection': {
            'partitionDistributionStrategy': 'CONSOLIDATED',
            'id': '53005a1d-b851-4485-9799-d81d06608f34',
            'name': '',
            'latestMaterializationState': 'NEW',
            'state': 'ACTIVE',
            'type': 'RAW'
          }
        },
        {
          'relationship': 'CHOSEN',
          'materialization': {
            'id': 'd22aea08-3f12-48a6-b019-0d7d2e89a0d1',
            'refreshChainStartTime': 0 // pre-1.3
          },
          'dataset': {
            'id': '5506693e-a14d-468f-930f-73ab798fc9b8',
            'path': ['@dremio', 'zips'],
            'type': 'PHYSICAL_DATASET_HOME_FILE'
          },
          'accelerationSettings': {
            'method': 'FULL',
            'refreshPeriod': 33061210698000,
            'gracePeriod': 33061210698000
          },
          'reflection': {
            'partitionDistributionStrategy': 'CONSOLIDATED',
            'id': '53005a1d-b851-4485-9799-d81d06608f34',
            'name': '',
            'latestMaterializationState': 'NEW',
            'state': 'ACTIVE',
            'type': 'RAW'
          }
        },
        {
          'relationship': 'MATCHED',
          'materialization': {
            'id': 'd22aea08-3f12-48a6-b019-0d7d2e89a0d1',
            'refreshChainStartTime': Date.now()
          },
          'dataset': {
            'id': '5506693e-a14d-468f-930f-73ab798fc9b8',
            'path': ['@dremio', 'zips'],
            'type': 'PHYSICAL_DATASET_HOME_FILE'
          },
          'accelerationSettings': {
            'method': 'FULL',
            'refreshPeriod': 33061210698000,
            'gracePeriod': 33061210698000
          },
          'reflection': {
            'details': {
              'partitionDistributionStrategy': 'CONSOLIDATED'
            },
            'id': {
              'id': '53005a1d-b851-4485-9799-d81d06608f34'
            },
            'name': '',
            'latestMaterializationState': 'NEW',
            'state': 'ACTIVE',
            'type': 'RAW'
          }
        }
      ]
    };
    commonProps = {
      ...minimalProps
    };
    context = {location: {}, loggedInUser: {admin: true}};

    wrapper = shallow(<ReflectionList {...commonProps}/>, {context});
  });

  it('should render with minimal props without exploding', () => {
    expect(shallow(<ReflectionList {...minimalProps}/>, {context})).to.have.length(1);
  });

  it('no link if not admin', () => {
    expect(shallow(<ReflectionList {...commonProps}/>, {context}).find('Link')).to.have.length(4);
    context.loggedInUser.admin = false;
    expect(shallow(<ReflectionList {...commonProps}/>, {context}).find('Link')).to.have.length(0);
    expect(shallow(<ReflectionList {...commonProps}/>, {context}).find('EllipsedText').first().props()).to.eql({
      'children': false,
      'text': '{"0":{"id":"Reflection.UnnamedReflection"}}'
    });
  });

  it('should render with common props without exploding', () => {
    expect(wrapper).to.have.length(1);
  });

  it('should describe the different relationships', () => {
    const items = wrapper.find('li');
    const descriptions = items.map(item => item.find('div').last().text());
    expect(descriptions).to.have.eql([
      '{"0":{"id":"Reflection.DidNotCoverQuery"}}',
      '{"0":{"id":"Reflection.Age"},"1":{"age":"<1s"}}',
      '', // pre-1.3 case
      '{"0":{"id":"Reflection.TooExpensive"}}'
    ]);
  });
});
