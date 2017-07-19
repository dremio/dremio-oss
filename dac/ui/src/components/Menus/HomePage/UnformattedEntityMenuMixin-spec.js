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

import { findMenuItemLinkByText } from 'testUtil';
import {UnformattedEntityMenu as UnformattedEntityMenuBase} from 'components/Menus/HomePage/UnformattedEntityMenu';
import UnformattedEntityMenuMixin from './UnformattedEntityMenuMixin';

@UnformattedEntityMenuMixin
class UnformattedEntityMenu extends UnformattedEntityMenuBase {}

describe('UnformattedEntityMenuMixin', () => {

  let commonProps;
  const context = {context: {location: {bar: 2, state: {foo: 1}}}};

  beforeEach(() => {
    commonProps = {
      entity: Immutable.fromJS({
        links: { self: '/abc' }
      }),
      href: {},
      closeMenu: sinon.stub()
    };
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<UnformattedEntityMenu {...commonProps} />, context);
    expect(wrapper).to.have.length(1);
  });

  it('should render menu items', () => {
    const wrapper = shallow(<UnformattedEntityMenu {...commonProps} />, context);
    expect(findMenuItemLinkByText(wrapper, 'Browse Contents')).to.have.length(1);
    expect(findMenuItemLinkByText(wrapper, 'Add Format')).to.have.length(1);
  });
});
