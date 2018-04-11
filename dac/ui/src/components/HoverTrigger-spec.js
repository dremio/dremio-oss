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
import HoverTrigger from './HoverTrigger';

describe('HoverTrigger', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      onEnter: sinon.spy(),
      onLeave: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<HoverTrigger {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should add mouseleave listener and call onLeave from props when item loses hover', () => {
    const instance = mount(<HoverTrigger {...commonProps}/>).instance();
    const event = new Event('mouseleave');
    sinon.spy(instance, 'watchMouseLeave');
    instance.componentDidMount();
    expect(instance.watchMouseLeave).to.be.called;
    expect(commonProps.onLeave).to.not.be.called;
    instance.hoverItem.dispatchEvent(event);
    expect(commonProps.onLeave).to.be.called;
  });

  it('should call onEnter from props when item hovered', () => {
    const wrapper = mount(<HoverTrigger {...commonProps}/>);
    expect(commonProps.onEnter).to.be.not.called;
    wrapper.simulate('mouseenter');
    expect(commonProps.onEnter).to.be.called;
  });
});
