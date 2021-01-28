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
import Immutable from 'immutable';

import LoginForm from './LoginForm';

import { LoginFormContainer } from './LoginFormContainer';

describe('LoginFormContainer', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      viewState: Immutable.Map()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<LoginFormContainer {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render LoginTitle and LoginForm', () => {
    const wrapper = shallow(<LoginFormContainer {...commonProps}/>);
    expect(wrapper.find('LoginTitle')).to.have.length(1);
    expect(wrapper.find(LoginForm)).to.have.length(1);
  });
});
