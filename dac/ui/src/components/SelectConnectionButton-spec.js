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

import SelectConnectionButton from './SelectConnectionButton';

describe('SelectConnectionButton', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      label: 'foo label',
      iconType: 'foo'
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<SelectConnectionButton {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<SelectConnectionButton {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render label and icon', () => {
    expect(wrapper.text()).to.contain(commonProps.label);
    expect(wrapper.find('SourceIcon').props().src).to.equal(`${commonProps.iconType}.svg`);
  });

  it('should render pillText if set', () => {
    wrapper.setProps({pillText: 'medicine'});
    expect(wrapper.text()).to.contain('medicine');
  });
});
