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

import Radio from './Radio';

describe('Radio', () => {
  let wrapper;
  let instance;
  beforeEach(() => {
    wrapper = shallow(<Radio radioValue='foo' value='foo'/>);
    instance = wrapper.instance();
  });

  it('should render checked when only when value=radioValue if props.checked is not defined', () => {
    expect(wrapper.find('Checkbox').props().checked).to.be.true;
    wrapper.setProps({value: 'bar'});
    expect(wrapper.find('Checkbox').props().checked).to.be.false;
  });

  it('should return props.checked if it is defined', () => {
    wrapper.setProps({checked: true, value: 'bar'});
    expect(wrapper.find('Checkbox').props().checked).to.be.true;

    wrapper.setProps({checked: false, value: 'foo'});
    expect(wrapper.find('Checkbox').props().checked).to.be.false;
  });

  describe('#renderDummyRadio', () => {
    it('should render dot only if isChecked', () => {
      expect(shallow(instance.renderDummyRadio(false)).find('div')).to.have.length(1);
      expect(shallow(instance.renderDummyRadio(true)).find('div')).to.have.length(2);
    });
  });
});
