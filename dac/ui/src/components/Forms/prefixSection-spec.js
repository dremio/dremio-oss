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
import { Component } from 'react';

import prefixSection from './prefixSection';

class FormSection extends Component {
  static getFields() {
    return ['foo'];
  }

  static validate(values) {
    return Object.keys(values).reduce((errors, key) => ({...errors, [key]: 'required'}), {});
  }

  static something = 'something';
}

describe('SectionPrefix', () => {
  it('Prefixes getFields without changing original', () => {
    const PrefixedSection = prefixSection('prefix')(FormSection);
    expect(PrefixedSection.getFields()).to.eql(['prefix.foo']);
    expect(FormSection.getFields()).to.eql(['foo']);
  });

  it('Prefixes validate without changing original', () => {
    const PrefixedSection = prefixSection('prefix')(FormSection);
    expect(PrefixedSection.validate({foo: 1})).to.eql({'prefix.foo': 'required'});
    expect(FormSection.validate({foo: 1})).to.eql({foo: 'required'});
  });

  it('can still access other statics', () => {
    const PrefixedSection = prefixSection('prefix')(FormSection);
    expect(PrefixedSection.something).to.eql(FormSection.something);
  });

  it('should unprefix fields passed as props', () => {
    const PrefixedSection = prefixSection('prefix')(FormSection);

    const wrapper = shallow(<PrefixedSection fields={{prefix: {foo: 'foo'}}}/>);
    expect(wrapper.find('FormSection').props().fields).to.eql({foo: 'foo'});
  });

});
