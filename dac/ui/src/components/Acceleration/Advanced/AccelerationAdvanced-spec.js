/*
 * Copyright (C) 2017 Dremio Corporation
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

import {AccelerationAdvanced} from './AccelerationAdvanced';
import AccelerationRaw from './AccelerationRaw';
import AccelerationAggregation from './AccelerationAggregation';

describe('AccelerationAdvanced', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {},
      acceleration: Immutable.Map()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationAdvanced {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find(AccelerationRaw)).to.have.length(1);
  });

  it('should render AccelerationAggregation if we select tab=Aggregation', () => {
    const wrapper = shallow(<AccelerationAdvanced {...commonProps}/>);
    wrapper.setState({activeTab: 'AGGREGATION'});
    expect(wrapper.find(AccelerationAggregation)).to.have.length(1);
  });

  describe('#getActiveTab()', () => {
    // other cases covered in minimal/common tests
    it('should render AccelerationAggregation as needed if we have such a layoutId', () => {
      const props = {
        ...commonProps,
        location: {state:{layoutId:'foo'}},
        acceleration: Immutable.fromJS({
          aggregationLayouts: {layoutList: [{id: {id: 'foo'}}]}
        })
      };
      const wrapper = shallow(<AccelerationAdvanced {...props}/>);
      wrapper.setState({activeTab: 'AGGREGATION'});
      expect(wrapper.find(AccelerationAggregation)).to.have.length(1); // use as canary
    });
  });
});
