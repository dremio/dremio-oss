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

import SimpleButton from './SimpleButton';

describe('SimpleButton', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      buttonStyle: 'primary'
    };
    commonProps = {
      ...minimalProps,
      onClick: sinon.spy(),
      buttonStyle: 'primary'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SimpleButton {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render button and children', () => {
    const wrapper = shallow(<SimpleButton {...commonProps}>foo</SimpleButton>);
    expect(wrapper.type()).to.eql('button');
    expect(wrapper.children().first().text()).to.equal('foo');
  });

  it('should set disabled attribute', () => {
    const wrapper = shallow(<SimpleButton {...commonProps}>foo</SimpleButton>);
    expect(wrapper.prop('disabled')).to.be.false;
    wrapper.setProps({disabled: true});
    expect(wrapper.prop('disabled')).to.be.true;
  });

  it('should set disabled when submitting', () => {
    const wrapper = shallow(<SimpleButton {...commonProps} submitting>foo</SimpleButton>);
    expect(wrapper.prop('disabled')).to.be.true;
  });

  it('should render spinner only when submitting', () => {
    const wrapper = shallow(<SimpleButton {...commonProps}>foo</SimpleButton>);
    expect(wrapper.find('FontIcon')).to.have.length(0);
    wrapper.setProps({submitting: true});
    expect(wrapper.find('FontIcon')).to.have.length(1);
  });
});
