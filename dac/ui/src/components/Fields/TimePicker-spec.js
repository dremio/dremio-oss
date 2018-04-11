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
import { TIME } from 'constants/DataTypes';
import TimePicker from './TimePicker';

describe('TimePicker', () => {
  const timeTest = '10:10:32.123';
  const commonProps = {
    columnType: TIME,
    value: timeTest
  };
  it('should render <input>', () => {
    const wrapper = shallow(<TimePicker {...commonProps} />);
    expect(wrapper.find('input')).to.have.length(1);
  });
  it('should run onBlur from props if value is valid', () => {
    commonProps.onBlur = sinon.spy();
    const wrapper = shallow(<TimePicker {...commonProps} />);
    const instance = wrapper.instance();
    instance.onBlur();
    expect(commonProps.onBlur.called).to.be.true;
  });
  it('should not run onBlur if value is not valid', () => {
    commonProps.onBlur = sinon.spy();
    const wrapper = shallow(<TimePicker {...commonProps} />);
    wrapper.setState({
      value: 'not valid time'
    });
    const instance = wrapper.instance();
    instance.onBlur();
    expect(commonProps.onBlur.called).to.be.false;
  });
});
