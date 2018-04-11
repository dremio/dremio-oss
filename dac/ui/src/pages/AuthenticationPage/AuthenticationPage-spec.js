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

import LoginForm from './components/LoginForm';
import { AuthenticationPage } from './AuthenticationPage';

describe('AuthenticationPage', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      checkAuth: sinon.spy(),
      logoutUser: sinon.spy(),
      checkForFirstUser: sinon.spy(),
      location: {}
    };
    commonProps = {
      ...minimalProps,
      user: Immutable.Map()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AuthenticationPage {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render loginForm or UnsupportedBrowserForm depending on state.showLoginForm', () => {
    const wrapper = shallow(<AuthenticationPage {...commonProps}/>);
    wrapper.setState({showLoginForm: true});
    expect(wrapper.find(LoginForm)).to.have.length(1);
    wrapper.setState({showLoginForm: false});
    expect(wrapper.name()).to.equal('UnsupportedBrowserForm');

  });

  describe('componentDidMount', () => {
    it('should call logoutUser and checkForFirstUser', () => {
      const wrapper = shallow(<AuthenticationPage {...commonProps}/>);
      wrapper.instance().componentDidMount();
      expect(commonProps.logoutUser).to.be.called;
      expect(commonProps.checkForFirstUser).to.be.called;
    });
  });
});
