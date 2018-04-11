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

import UsersView from './UsersView';

describe('UsersView', () => {
  let commonProps;
  let context;
  beforeEach(() => {
    commonProps = {
      users: Immutable.fromJS([
        {
          userConfig: { // todo: why is this nested?
            email: 'email@domain.com',
            firstName: 'First',
            lastName: 'Last',
            userName: 'userName'
          }
        }
      ]),
      removeUser: () => {}
    };
    context = { location: {}, loggedInUser: { userName: 'userName'} };
  });

  it('render elements', () => {
    const wrapper = shallow(<UsersView {...commonProps}/>, {context});
    expect(wrapper.find('#admin-user')).have.length(1);
    expect(wrapper.find('.admin-header')).have.length(1);
    expect(wrapper.find('.filter')).have.length(1);
    expect(wrapper.find('StatefulTableViewer')).have.length(1);
  });

  describe('#getTableData', () => {
    it('should return correct data with delete button when user name is not equal current logged in user', () => {
      context.loggedInUser.userName = 'foo';
      const instance = shallow(<UsersView {...commonProps}/>, {context}).instance();
      const result = instance.getTableData().toJS()[0].data[3].props.children[1].type;
      expect(result).to.be.eql('button');
    });

    it('should return correct data without delete button when user has current logged in user name', () => {
      const instance = shallow(<UsersView {...commonProps}/>, {context}).instance();
      const result = instance.getTableData().toJS()[0].data[3].props.children[1];
      expect(result).to.be.false;
    });
  });
});

