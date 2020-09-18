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
import { shallow, mount } from 'enzyme';
import { expect } from 'chai';
import TooltipEnabledLabel from './TooltipEnabledLabel';


describe('TooltipEnabledLabel', () => {
  let wrapper;
  let instance;
  const minimalProps = {
    label: 'Test label',
    tooltip: 'Test tooltip',
    labelBefore: true
  };
  beforeEach(() => {
    wrapper = shallow(<TooltipEnabledLabel {...minimalProps}/>);
    instance = wrapper.instance();
  });

  it('should render label enabled with tooltip with minimal props ', () => {
    expect(wrapper).to.have.length(1);
  });

  it('should render label Test label and tooltip as Test tooltip', () => {
    expect(wrapper.text()).to.be.contain('Test label');
    expect(instance.props.tooltip).to.be.equal('Test tooltip');
  });

  it('should mouse enter tooltip', () => {
    expect(instance.state.hover).to.false;
    wrapper.find('span').at(0).simulate('mouseenter');
    expect(instance.state.hover).to.true;
  });

  it('should mouse leave tooltip', () => {
    expect(instance.state.hover).to.false;
    wrapper.find('span').at(0).simulate('mouseenter');
    expect(instance.state.hover).to.true;
    wrapper.find('span').at(0).simulate('mouseleave');
    expect(instance.state.hover).to.be.false;
  });

  it('should tooltip target be span with ref target', () => {
    wrapper.setProps({...minimalProps, labelBefore: false, tooltip: null});
    instance = mount(<TooltipEnabledLabel {...minimalProps}/>).instance();
    expect(instance.refs.target).to.not.undefined;
    expect(instance.refs.target.innerHTML).to.eq('Test label');
  });
});
