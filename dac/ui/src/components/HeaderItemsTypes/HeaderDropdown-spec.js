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

import Menu from 'components/Menus/Menu';

import HeaderDropdown from './HeaderDropdown';

describe('HeaderDropdown', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      name: 'theName',
      menu: <Menu></Menu>
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<HeaderDropdown {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<HeaderDropdown {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render name, and Popover and Menu', () => {
    const selectViewWrapper = wrapper.find('SelectView');

    expect(selectViewWrapper).to.have.length(1);
    expect(shallow(selectViewWrapper.prop('content')).text()).to.contain(commonProps.name);
  });
});
