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

import TextFormatForm from './TextFormatForm';

describe('TextFormatForm', () => {
  let minimalProps;
  let fieldDelimiter;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      fields: {
        Text: {
          fieldDelimiter: (fieldDelimiter = fieldDelimiter = {
            onChange: sinon.spy(),
            value: '\n'
          }),
          lineDelimiter: {
            onChange: sinon.spy(),
            value: '\n'
          },
          quote: {
            onChange: sinon.spy(),
            value: '"'
          },
          comment: {
            onChange: sinon.spy(),
            value: '#'
          },
          escape: {
            onChange: sinon.spy(),
            value: '`'
          }
        }
      }
    };
    wrapper = shallow(<TextFormatForm {...minimalProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    expect(wrapper).to.have.length(1);
    expect(wrapper.find('FormatField')).to.have.length(5);
    expect(wrapper.find('Checkbox')).to.have.length(3);
  });
  describe('#onDelimiterChange', () => {
    it('should remove JSON escaping', () => {
      const onDelimiterChange = instance.onDelimiterChange(fieldDelimiter);

      onDelimiterChange('\\n');
      expect(fieldDelimiter.onChange).to.be.calledWith('\n');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\\t\\n');
      expect(fieldDelimiter.onChange).to.be.calledWith('\t\n');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\t');
      expect(fieldDelimiter.onChange).to.be.calledWith('\t');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\"');
      expect(fieldDelimiter.onChange).to.be.calledWith('\"');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\\u0000');
      expect(fieldDelimiter.onChange).to.be.calledWith('\u0000');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\\\\u0000');
      expect(fieldDelimiter.onChange).to.be.calledWith('\\u0000');
      fieldDelimiter.onChange.reset();
    });
    it('should treat all-slashes as plain text', () => {
      const onDelimiterChange = instance.onDelimiterChange(fieldDelimiter);

      onDelimiterChange('\\\\');
      expect(fieldDelimiter.onChange).to.be.calledWith('\\\\');
      fieldDelimiter.onChange.reset();
    });

    it('should treat invalid JSON as plain text', () => {
      const onDelimiterChange = instance.onDelimiterChange(fieldDelimiter);

      onDelimiterChange('\\"');
      expect(fieldDelimiter.onChange).to.be.calledWith('\\"');
      fieldDelimiter.onChange.reset();

      onDelimiterChange('\\uBADX');
      expect(fieldDelimiter.onChange).to.be.calledWith('\\uBADX');
      fieldDelimiter.onChange.reset();
    });

  });
  describe('#getDelimiterValue', () => {
    it('should add JSON escaping', () => {
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\n');
      fieldDelimiter.value = '\n\t';
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\n\\t');
      fieldDelimiter.value = '"';
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('"');
      fieldDelimiter.value = '\u0000';
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\u0000');
      fieldDelimiter.value = '\\foo';
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\\\foo');
      fieldDelimiter.value = '\u200a'; // not naturally escapsed by JSON.stringify
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\u200a');
    });

    it('should not add JSON escaping for all-slashes string', () => {
      fieldDelimiter.value = '\\\\';
      expect(instance.getDelimiterValue(fieldDelimiter)).to.eql('\\\\');
    });
  });
});
