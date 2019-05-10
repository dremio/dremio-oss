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

import FieldWithError from './FieldWithError';

describe('FieldWithError', () => {
  it('should render children', () => {
    const wrapper = shallow(<FieldWithError><input/></FieldWithError>);
    expect(wrapper.contains(<input />)).to.equal(true);
    expect(wrapper.find('label')).to.have.length(0);
  });

  it('should render label', () => {
    const wrapper = shallow(<FieldWithError label='label'><input/></FieldWithError>);
    expect(wrapper.find('label').text()).to.equal('label');
  });

  it('should not render error if there is none', () => {
    const wrapper = shallow(<FieldWithError><input/></FieldWithError>);
    wrapper.instance().refs = { target: {} }; // simulate refs
    expect(wrapper.find('Tooltip').prop('target')()).to.be.null;
  });

  it('should render error when there is one', () => {
    const props = {
      touched: true,
      error: 'error'
    };
    const wrapper = shallow(<FieldWithError {...props}><input/></FieldWithError>);
    const refObj = {};
    wrapper.instance().refs = { target: refObj }; // simulate refs
    expect(wrapper.find('Tooltip').prop('target')()).to.equal(refObj);
  });

  it('should throw when there is no child', () => {
    expect(() => {
      shallow(<FieldWithError />);
    }).to.throw(Error);
  });
});
