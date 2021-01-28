/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import LoginFormContainer from './components/LoginFormContainer';
import { AuthenticationPage } from './AuthenticationPage';

describe('AuthenticationPage', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      checkAuth: sinon.spy(),
      checkForFirstUser: sinon.spy(),
      location: {}
    };
  });

  it('should render loginForm or UnsupportedBrowserForm depending on state.showLoginForm', () => {
    const wrapper = shallow(<AuthenticationPage {...commonProps}/>);
    wrapper.setState({showLoginForm: true});
    expect(wrapper.find(LoginFormContainer)).to.have.length(1);
    wrapper.setState({showLoginForm: false});
    expect(wrapper.name()).to.equal('UnsupportedBrowserForm');

  });
});
