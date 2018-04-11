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
import { shallow, mount } from 'enzyme';

import RealTimeTimer from './RealTimeTimer';

describe('RealTimeTimer', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      startTime: 1000
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<RealTimeTimer {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  it('should start timer on mount', () => {
    const wrapper = mount(<RealTimeTimer {...commonProps}/>);
    expect(wrapper.instance().intervalId).to.not.be.undefined;
    wrapper.instance().componentWillUnmount();
  });

  it('should render time since props.startTime', () => {
    sinon.stub(Date, 'now').returns(3000);
    const wrapper = shallow(<RealTimeTimer {...commonProps}/>);
    expect(wrapper.text()).to.equal('2000');
    Date.now.restore();
    wrapper.instance().componentWillUnmount();
  });

  it('should update every updateInterval', () => {
    const clock = sinon.useFakeTimers(10000);
    const wrapper = mount(<RealTimeTimer {...commonProps}/>);
    expect(wrapper.text()).to.equal('9000');

    clock.tick(1000);
    expect(wrapper.text()).to.equal('10000');

    clock.tick(1000);
    expect(wrapper.text()).to.equal('11000');

    clock.restore();
    wrapper.instance().componentWillUnmount();
  });
});
