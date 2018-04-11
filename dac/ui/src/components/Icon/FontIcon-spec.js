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
import React from 'react';
import { shallow } from 'enzyme';

import FontIcon, { FullFontIcon } from './FontIcon';

describe('FontIcon', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      type: 'FolderConvert'
    };
    commonProps = {
      type: 'Database',
      onClick: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<FontIcon {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should cache created Component only when only props are type and theme', () => {
    sinon.spy(FontIcon.prototype, 'createFastIconComponent');
    let wrapper = shallow(<FontIcon type='Foo1'/>);
    expect(FontIcon.prototype.createFastIconComponent).to.be.calledOnce;
    expect(wrapper.find(FullFontIcon)).to.have.length(0);

    wrapper = shallow(<FontIcon type='Foo1'/>);
    expect(FontIcon.prototype.createFastIconComponent).to.be.calledOnce;
    expect(wrapper.find(FullFontIcon)).to.have.length(0);

    wrapper = shallow(<FontIcon type='Foo2'/>);
    expect(FontIcon.prototype.createFastIconComponent).to.be.calledTwice;
    expect(wrapper.find(FullFontIcon)).to.have.length(0);

    wrapper = shallow(<FontIcon type='Foo2' theme={{}}/>);
    expect(FontIcon.prototype.createFastIconComponent).to.be.calledTwice;
    expect(wrapper.find(FullFontIcon)).to.have.length(0);

    wrapper = shallow(<FontIcon type='Foo2' onClick={() => {}}/>);
    expect(FontIcon.prototype.createFastIconComponent).to.be.calledTwice;
    expect(wrapper.find(FullFontIcon)).to.have.length(1);

    FontIcon.prototype.createFastIconComponent.restore();
  });

  it('should render FullFontIcon if there are more props than just type', () => {
    const wrapper = shallow(<FontIcon {...commonProps}/>);
    expect(wrapper.find(FullFontIcon)).to.have.length(1);
  });

  it('should pass props.theme to FastIcon component', () => {
    const theme1 = {Icon: {iconStyle: 1}, Container: {containerStyle: 1}};
    const wrapper1 = shallow(<FontIcon {...minimalProps} theme={theme1}/>);
    expect(wrapper1.props().theme).to.equal(theme1);

    const theme2 = {Icon: {iconStyle: 2}, Container: {containerStyle: 2}};
    const wrapper2 = shallow(<FontIcon {...minimalProps} theme={theme2}/>);
    expect(wrapper2.props().theme).to.equal(theme2);
  });

  describe('#createFastIconComponent', () => {
    it('should return Component that sets style and className', () => {
      const instance = shallow(<FontIcon {...minimalProps}/>).instance();
      const iconStyle = {iconStyle: 1};
      const containerStyle = {containerStyle: 1};

      const component = instance.createFastIconComponent('Bar', iconStyle, containerStyle);
      const wrapper = shallow(React.createElement(component));
      expect(wrapper.find('span').at(1).props().className).to.eql('fa Bar icon-type');
      expect(wrapper.find('span').at(0).props().style).to.equal(containerStyle);
      expect(wrapper.find('span').at(1).props().style).to.equal(iconStyle);
    });

    it('should allow overriding theme', () => {
      const instance = shallow(<FontIcon {...minimalProps}/>).instance();
      const theme = {Icon: {iconStyle: 1}, Container: {containerStyle: 1}};

      const component = instance.createFastIconComponent('Baz');
      const wrapper = shallow(React.createElement(component, {theme}));
      expect(wrapper.find('span').at(0).props().style).to.equal(theme.Container);
      expect(wrapper.find('span').at(1).props().style).to.equal(theme.Icon);
    });
  });
});
