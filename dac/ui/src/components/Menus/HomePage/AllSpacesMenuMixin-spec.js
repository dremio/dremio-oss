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

import { findMenuItemLinkByText, findMenuItemByText } from 'testUtil';
import {AllSpacesMenu as AllSpacesMenuBase} from 'components/Menus/HomePage/AllSpacesMenu';
import { getRenderEntityLinkContent } from '@app/pages/HomePage/components/EntityLink-spec';
import { EntityLinkProvider } from '@app/pages/HomePage/components/EntityLink';
import AllSpacesMenuMixin from './AllSpacesMenuMixin';

@AllSpacesMenuMixin
class AllSpacesMenu extends AllSpacesMenuBase {}

describe('AllSpacesMenuMixin', () => {
  let minimalProps;
  let commonProps;

  const context = {context: {location: {bar: 2, state: {foo: 1}}}};
  beforeEach(() => { // todo: DRY
    minimalProps = {
      item: Immutable.fromJS({
        id: 'simpleSpace',
        links: {
          self: '/asd'
        }
      }),
      closeMenu: sinon.stub(),
      removeItem: sinon.stub(),
      showConfirmationDialog: sinon.stub()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render menu items', () => {
    const wrapper = shallow(<AllSpacesMenu {...commonProps} />, context);
    const browse = getRenderEntityLinkContent(wrapper.find(EntityLinkProvider));
    expect(browse.props.text).to.be.equal('Browse');
    expect(findMenuItemLinkByText(wrapper, 'Edit Details')).to.have.length(1);
    expect(findMenuItemByText(wrapper, 'Remove Space')).to.have.length(1);
  });
});
