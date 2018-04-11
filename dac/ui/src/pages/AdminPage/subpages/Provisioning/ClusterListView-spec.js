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

import ClusterListView from './ClusterListView';

describe('ClusterListView', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
    };
    commonProps = {
      ...minimalProps,
      provisions: Immutable.fromJS([
        {id: {id: '1'}},
        {id: {id: '2'}}
      ])
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ClusterListView {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SingleCluster components based on props.provisions', () => {
    const wrapper = shallow(<ClusterListView {...commonProps}/>);
    expect(wrapper).to.have.length(1);
    const clustersWrapper = wrapper.find('Connect(SingleCluster)');
    expect(clustersWrapper).to.have.length(commonProps.provisions.size);
    expect(clustersWrapper.get(0).props.entity).to.eql(commonProps.provisions.get(0));
  });
});
