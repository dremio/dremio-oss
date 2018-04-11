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

import { DatasetAccelerationButton } from './DatasetAccelerationButton';

describe('DatasetAccelerationButton', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      getDatasetAccelerationRequest: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetAccelerationButton {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render flame icon when acceleration enabled', () => {
    const wrapper = shallow(<DatasetAccelerationButton {...minimalProps}/>);
    wrapper.setProps({acceleration: Immutable.fromJS({ enabled: true})});
    expect(wrapper.find('FontIcon').props().type).to.be.eql('Flame');
  });

  it('should render nothing when acceleration is disabled', () => {
    const wrapper = shallow(<DatasetAccelerationButton {...minimalProps}/>);
    expect(wrapper.children()).to.have.length(0);
  });
});
