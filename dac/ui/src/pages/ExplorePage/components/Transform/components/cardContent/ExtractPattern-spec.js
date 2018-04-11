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
import ExtractPattern from './ExtractPattern';

describe('ExtractPattern', () => {
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExtractPattern />);
    expect(wrapper).to.have.length(1);
  });

  describe('Pattern Index input', () => {
    it('should render Pattern Index field if pattern.value.value.value = INDEX', () => {
      const props = {
        fields: {
          pattern: {
            value: { value: { value: 'INDEX' } }
          }
        }
      };
      const wrapper = shallow(<ExtractPattern {...props}/>);
      expect(wrapper.find('input[data-qa="PatternIndex"]')).to.have.length(1);
    });

    it('should render Pattern Index field if pattern.value.value.value = CAPTURE_GROUP', () => {
      const props = {
        fields: {
          pattern: {
            value: { value: { value: 'CAPTURE_GROUP' } }
          }
        }
      };
      const wrapper = shallow(<ExtractPattern {...props}/>);
      expect(wrapper.find('input[data-qa="PatternIndex"]')).to.have.length(1);
    });

    it(`should not render Pattern Index field if
        pattern.value.value.value doesn\'t equal CAPTURE_GROUP or INDEX`, () => {
      const props = {
        fields: {
          pattern: {
            value: { value: { value: 'LAST' } }
          }
        }
      };
      const wrapper = shallow(<ExtractPattern {...props}/>);
      expect(wrapper.find('input[data-qa="PatternIndex"]')).to.have.length(0);
    });
  });
});
