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
import { shallow } from 'enzyme';
import OverView from './OverView';

describe('OverView', () => {

  const minimalProps = {
    scansForFilter: [],
    jobSummary: [],
    reflectionsUsed: [],
    reflectionsNotUsed: [],
    queriedDataSet: [],
    duration: {
      value: {
        isAcceleration: false
      }
    },
    location: {},
    jobDetails: Immutable.fromJS({
      queryText: 'SELECT * FROM nation',
      durationDetails: [],
      id: 'test id',
      duration: 45,
      accelerated: false,
      queryType: 'Preview',
      startTime: 1622102233268,
      queryUser: 'dremio',
      wlmQueue: 'queue',
      queriedDatasets: [],
      scannedDatasets: [],
      inputBytes: '20',
      outputBytes: '20',
      inputRecords: '25',
      outputRecords: '20'
    })
  };

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<OverView {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
});
