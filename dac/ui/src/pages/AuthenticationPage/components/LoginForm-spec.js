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

import { minimalFormProps } from 'testUtil';

import { LoginForm } from './LoginForm';

describe('LoginForm', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      ...minimalFormProps(['userName', 'password']),
      loginUser: sinon.stub().returns(Promise.resolve({error: false})),
      replace: sinon.spy(),
      viewState: Immutable.Map(),
      location: {}
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<LoginForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render LoginTitle and 2 Fields', () => {
    const wrapper = shallow(<LoginForm {...commonProps}/>);
    expect(wrapper.find('LoginTitle')).to.have.length(1);
    expect(wrapper.find('FieldWithError')).to.have.length(2);
  });

  describe('#submit', () => {
    it('should call loginUser', () => {
      const instance = shallow(<LoginForm {...commonProps}/>).instance();
      const promise = instance.submit();
      expect(commonProps.loginUser).to.be.called;
      return promise.then(() => {
        expect(commonProps.replace).to.be.called;
      });
    });
  });
});
