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

import { Capabilities } from '@app/utils/authUtils';

import { HeaderButtons as HeaderButtonsBase } from './HeaderButtons';
import HeaderButtonsMixin from './HeaderButtonsMixin';

@HeaderButtonsMixin
class HeaderButtons extends HeaderButtonsBase {}

describe('HeaderButtonsMixin', () => {
  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      toggleVisibility: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      rootEntityType: 'home'
    };
    context = {
      location: {}
    };
  });

  describe('getSpaceSettingsButtons', () => {
    it('should return folder config by default', () => {
      const wrapper = shallow(<HeaderButtons {...commonProps}/>, {context});
      const instance = wrapper.instance();

      expect(JSON.stringify(instance.getSpaceSettingsButtons())).to.eql(JSON.stringify([
        {
          qa: 'add-folder',
          iconType: 'Folder',
          to: {...context.location, state: {modal: 'AddFolderModal'}},
          isAdd: true
        }
      ]));
    });

    it('should return config for setting button if entityType  is space', () => {
      const wrapper = shallow(<HeaderButtons {...commonProps}/>, {context});
      const instance = wrapper.instance();

      wrapper.setProps({
        entity: Immutable.fromJS({ entityType: 'space', id: 'Prod-sample' })
      });

      expect(instance.getSpaceSettingsButtons()).to.eql([
        {
          qa: 'settings',
          iconType: 'Settings',
          to: {...context.location, state: {modal: 'SpaceModal', entityId: 'Prod-sample'}},
          authRule: {
            isAdmin: true,
            capabilities: [Capabilities.manageSpaces]
          }
        },
        {
          qa: 'add-folder',
          iconType: 'Folder',
          to: {...context.location, state: {modal: 'AddFolderModal'}},
          isAdd: true
        }
      ]);
    });
  });

  describe('getSourceSettingsButtons', () => {
    it('should return config for setting button if entityType is source', () => {
      const wrapper = shallow(<HeaderButtons {...commonProps}/>, {context});
      const instance = wrapper.instance();

      expect(instance.getSourceSettingsButtons()).to.eql([]);

      wrapper.setProps({
        entity: Immutable.fromJS({
          entityType: 'source',
          name: 'Mongo',
          type: 'mongo'
        })
      });

      expect(instance.getSourceSettingsButtons()).to.eql([{
        qa: 'settings',
        iconType: 'Settings',
        to: {
          ...context.location, state: {
            modal: 'EditSourceModal',
            query: {name: 'Mongo', type: 'mongo'}
          }
        },
        authRule: {
          isAdmin: true
        }
      }]);
    });
  });
});
