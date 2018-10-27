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

import Checkbox from './Checkbox';

describe('Checkbox', () => {

  let wrapper;
  let instance;
  const minimalProps = {
  };
  beforeEach(() => {
    wrapper = shallow(<Checkbox {...minimalProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<Checkbox {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render props.label', () => {
    wrapper.setProps({label: 'someLabel'});
    expect(wrapper.text()).to.contain('someLabel');
  });

  it('should render props.inputType', () => {
    wrapper.setProps({inputType: 'someInputType'});
    expect(wrapper.find('input').props().type).to.equal('someInputType');
  });

  it('should render label first only if labelBefore=true', () => {
    wrapper.setProps({labelBefore: true, label: 'someLabel'});
    expect(wrapper.children().at(0).text()).to.equal('someLabel');

    wrapper.setProps({labelBefore: false, label: 'someLabel'});
    expect(wrapper.children().at(2).text()).to.equal('someLabel');
  });

  it('should render checked when checked is truthy regardless of value', () => {
    expect(wrapper.find('input').props().checked).to.be.falsy;
    wrapper.setProps({value: true});
    expect(wrapper.find('input').props().checked).to.be.falsy;
    wrapper.setProps({checked:false, value: true});
    expect(wrapper.find('input').props().checked).to.be.falsy;
    wrapper.setProps({checked:true});
    expect(wrapper.find('input').props().checked).to.be.true;
    wrapper.setProps({checked:true, value: false});
    expect(wrapper.find('input').props().checked).to.be.true;
  });

  describe('#renderDummyCheckbox', () => {
    it('should render ✔ only if isChecked', () => {
      expect(shallow(instance.renderDummyCheckbox(false)).text()).to.not.contain('✔');
      expect(shallow(instance.renderDummyCheckbox(true)).text()).to.contain('✔');
    });
  });
});
