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
import { Users } from './Users';
describe('Users', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {query: {}},
      searchUsers: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      showConfirmationDialog: sinon.spy(),
      removeUser: sinon.stub().returns(Promise.resolve())
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Users {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#handleRemoveUser', () => {
    it('should show confirmation dialog before remove', () => {
      const instance = shallow(<Users {...commonProps} />).instance();
      sinon.spy(instance, 'removeUser');
      instance.handleRemoveUser();
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(instance.removeUser).to.not.be.called;
    });

    it('should call remove user when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const user = {user: '1'};
      const instance = shallow(<Users {...props} />).instance();
      sinon.spy(instance, 'removeUser');
      instance.handleRemoveUser(user);
      expect(instance.removeUser).to.be.called;
      expect(props.removeUser).to.be.calledWith(user);
    });
  });
});
