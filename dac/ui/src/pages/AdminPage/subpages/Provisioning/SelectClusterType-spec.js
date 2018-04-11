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

import SelectClusterType from './SelectClusterType';

describe('SelectClusterType', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onSelectClusterType: sinon.spy(),
      clusters: Immutable.List()
    };
    commonProps = {
      ...minimalProps,
      clusters: Immutable.fromJS([{
        label: 'cluster1',
        clusterType: 'clusterType1',
        iconType: 'iconType1',
        connected: true
      }, {
        label: 'cluster2',
        clusterType: 'clusterType2',
        iconType: 'iconType2',
        connected: false
      }])
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectClusterType {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render one SelectConnectionButton for each cluster', () => {
    const wrapper = shallow(<SelectClusterType {...commonProps}/>);
    expect(wrapper.find('SelectConnectionButton')).to.have.length(commonProps.clusters.size);
  });
});
