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

import Exact from './Exact';

describe('Exact', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      replaceValues: {value: ['21'], initialValue: ['21'], onChange: sinon.spy()},
      replaceNull: {value: false, initialValue: false, onChange: sinon.spy()},
      columnType: 'TEXT'
    };
  });

  it('renders', () => {
    const wrapper = shallow(<Exact {...commonProps}/>);
    expect(wrapper.type()).to.eql('div');
    expect(wrapper.hasClass('exact')).to.equal(true);
  });

  describe('Radio state', () => {

    it('should check null Radio if value is null', () => {
      commonProps.replaceNull.value = true;
      const wrapper = shallow(<Exact {...commonProps}/>);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([false, true]);
    });

    it('should check value Radio if value is not null', () => {
      const wrapper = shallow(<Exact {...commonProps}/>);
      expect(wrapper.find('Radio').map(x => x.props().checked)).to.eql([true, false]);
    });

  });

  describe('TextField state', () => {

    it('should render a TextField with props from this.props.fields.replaceValues', () => {
      const wrapper = shallow(<Exact {...commonProps}/>);
      const TextField = wrapper.find('TextField').props();
      expect(TextField.value).to.eql(commonProps.replaceValues.value);
      expect(TextField.onChange).to.eql(commonProps.replaceValues.onChange);
    });

    it('should TextField be disabled if check is null', () => {
      commonProps.replaceNull.value = true;
      const wrapper = shallow(<Exact {...commonProps}/>);
      const TextField = wrapper.find('TextField').props();
      expect(TextField.disabled).to.eql(true);
    });

  });

});
