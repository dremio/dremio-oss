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

import SignupForm from './components/SignupForm';
import SignupPage from './SignupPage';

describe('SignupPage', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {}
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SignupPage {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SignupForm or UnsupportedBrowserForm depending on state.showSignupForm', () => {
    const wrapper = shallow(<SignupPage {...commonProps}/>);
    wrapper.setState({showSignupForm: true});
    expect(wrapper.find(SignupForm)).to.have.length(1);
    wrapper.setState({showSignupForm: false});
    expect(wrapper.name()).to.equal('UnsupportedBrowserForm');
  });
});
