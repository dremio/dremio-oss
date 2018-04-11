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

import PasswordField from './PasswordField';

describe('PasswordField', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
    };
    commonProps = {
      ...minimalProps,
      initialFocus: true,
      error: '',
      onChange: sinon.spy(),
      touched: false,
      disabled: false,
      default: '',
      style: {},
      value:''
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<PasswordField {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<PasswordField {...commonProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render TextField with common props', () => {
    const wrapper = shallow(<PasswordField {...commonProps} />);
    expect(wrapper.find('TextField')).to.have.length(1);
  });

  it('should render eye icon when value not empty', () => {
    const props = {...commonProps, value: 'val1'};
    const wrapper = shallow(<PasswordField {...props} />);
    expect(wrapper.find('FontIcon')).to.have.length(1);
  });

  it('should not render eye icon when value is empty', () => {
    const wrapper = shallow(<PasswordField {...commonProps} />);
    expect(wrapper.find('FontIcon')).to.have.length(0);
  });

  it('should render TextField with text upon toggle (and back again)', () => {
    const wrapper = shallow(<PasswordField {...commonProps} />);
    expect(wrapper.find('TextField').props().type).to.equal('password');
    wrapper.instance().togglePassView();
    wrapper.update();
    expect(wrapper.find('TextField').props().type).to.equal('text');
    wrapper.instance().togglePassView();
    wrapper.update();
    expect(wrapper.find('TextField').props().type).to.equal('password');
  });
});
