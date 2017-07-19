/*
 * Copyright (C) 2017 Dremio Corporation
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

import MultiplierField from './MultiplierField';

const MULTIPLIERS = new Map([
  ['B', 1024 ** 0],
  ['KB', 1024 ** 1],
  ['MB', 1024 ** 2],
  ['GB', 1024 ** 3],
  ['TB', 1024 ** 4]
]);

describe('MultiplierField', () => {

  let minimalProps, commonProps;
  beforeEach(() => {
    minimalProps = {
      unitMultipliers: MULTIPLIERS
    };
    commonProps = {
      ...minimalProps,
      value: 1024,
      onChange: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<MultiplierField {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with commonProps props without exploding', () => {
    const wrapper = shallow(<MultiplierField {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with proper units', () => {
    const wrapper = shallow(<MultiplierField {...commonProps} value={1024 ** 3}/>);
    expect(wrapper.find('PrevalidatedTextField').props().value).to.equal('' + 1);
    expect(wrapper.find('Select').props().value).to.equal('GB');
  });

  it('should render with lower units in cases where there would be a long decimal', () => {
    const wrapper = shallow(<MultiplierField {...commonProps} value={1024 ** 3 + 1024 ** 2}/>);
    expect(wrapper.find('PrevalidatedTextField').props().value).to.equal('' + 1025);
    expect(wrapper.find('Select').props().value).to.equal('MB');
  });

  it('should render with proper units after external value change', () => {
    const wrapper = shallow(<MultiplierField {...commonProps} value={1024 ** 4}/>);
    expect(wrapper.find('PrevalidatedTextField').props().value).to.equal('' + 1);
    expect(wrapper.find('Select').props().value).to.equal('TB');
  });

  it('#handleTextChange() should update state.unit, call onChange', () => {
    const wrapper = shallow(<MultiplierField {...commonProps} />);
    wrapper.instance().handleTextChange('' + 1024 ** 2);
    expect(commonProps.onChange).to.have.been.calledWith(1024 ** 3);
    wrapper.setProps({...commonProps, value: 1024 ** 3});

    // stays in KB
    expect(wrapper.state().unit).to.equal('KB');
    expect(wrapper.find('PrevalidatedTextField').props().value).to.equal('' + 1024 ** 2);
    expect(wrapper.find('Select').props().value).to.equal('KB');
  });

  it('#handleSelectChange() should update state.unit', () => {
    const wrapper = shallow(<MultiplierField {...commonProps} />);
    wrapper.instance().handleSelectChange('B');
    expect(commonProps.onChange).to.not.have.been.called;
    expect(wrapper.find('PrevalidatedTextField').props().value).to.equal('' + 1024);
    expect(wrapper.find('Select').props().value).to.equal('B');
  });
});
