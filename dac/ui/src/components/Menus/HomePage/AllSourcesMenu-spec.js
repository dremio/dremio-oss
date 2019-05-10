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
import Immutable from 'immutable';
import { findMenuItemLinkByText, findMenuItemByText } from 'testUtil';
import { RestrictedArea } from '@app/components/Auth/RestrictedArea';
import { AllSourcesMenu } from './AllSourcesMenu';

describe('AllSourcesMenu', () => {
  let minimalProps;
  let commonProps;
  let contextTypes;
  beforeEach(() => {
    minimalProps = {
      item: Immutable.fromJS({
        links: {
          self: ''
        }
      }),
      removeItem: sinon.spy(),
      showConfirmationDialog: sinon.spy(),
      closeMenu: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
    contextTypes = {
      location: {}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AllSourcesMenu {...minimalProps}/>, {context: contextTypes});
    expect(wrapper).to.have.length(1);
  });

  it('renders edit and remove buttons in restricted area for admins', () => {
    const restrictedAreaWrapper = shallow(<AllSourcesMenu {...minimalProps}/>, {context: contextTypes}).find(RestrictedArea);
    expect(restrictedAreaWrapper).to.have.length(1, 'RestrictedArea must be rendered');
    expect(restrictedAreaWrapper.prop('rule')).to.be.eql({
      isAdmin: true
    }, 'We should allow access for a restricted area only for admins');

    expect(findMenuItemByText(restrictedAreaWrapper, 'Remove Source')).to.have.length(1,
      'Remove menu is rendered in a restricted area');
    expect(findMenuItemLinkByText(restrictedAreaWrapper, 'Edit Details')).to.have.length(1,
      'Edit menu is rendered in a restricted area');
  });

  describe('#handleRemoveSource', () => {
    it('should show confirmation dialog before removing', () => {
      const wrapper = shallow(<AllSourcesMenu {...commonProps} />, {context: contextTypes});
      const instance = wrapper.instance();
      instance.handleRemoveSource();
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.removeItem).to.not.be.called;
    });

    it('should call remove source when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const wrapper = shallow(<AllSourcesMenu {...props}/>, {context: contextTypes});
      const instance = wrapper.instance();
      instance.handleRemoveSource();
      expect(props.removeItem).to.be.called;
    });
  });
});
