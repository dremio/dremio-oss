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
import ListItem from './ListItem';

describe('ListItem', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ListItem {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render common props', () => {
    const wrapper = shallow(<ListItem {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });
});
