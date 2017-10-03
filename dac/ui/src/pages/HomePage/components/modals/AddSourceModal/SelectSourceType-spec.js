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

import { sourceProperties } from 'dyn-load/constants/sourceTypes';

import SelectSourceType from './SelectSourceType';

describe('SelectSourceType', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      onSelectSource: sinon.spy(),
      onAddSampleSource: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<SelectSourceType {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<SelectSourceType {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectConnectionButton for each sourceType', () => {
    expect(wrapper.find('SelectConnectionButton')).to.have.length(sourceProperties.length + 1);
  });
});
