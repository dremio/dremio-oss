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

import ValidityIndicator from './ValidityIndicator';

describe('ValidityIndicator', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      isValid: true
    };
  });

  it('should render with minimal props without exploding (false case)', () => {
    const wrapper = shallow(<ValidityIndicator {...minimalProps}/>);
    expect(wrapper.find('FontIcon').props().type).to.equal('Flame-Disabled');
  });

  it('should render with common props without exploding (true case)', () => {
    const wrapper = shallow(<ValidityIndicator {...commonProps}/>);
    expect(wrapper.find('FontIcon').props().type).to.equal('Flame');
  });
});
