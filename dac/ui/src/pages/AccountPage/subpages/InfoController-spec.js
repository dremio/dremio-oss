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
import ApiUtils from 'utils/apiUtils/apiUtils';

import { InfoController } from './InfoController';

const commonFormValues = {
  userName: {value: 'user'},
  firstName: {value: 'firstName'},
  lastName: {value: 'lastName'},
  email: {value: 'some@example.com'},
  version: {value: 0},
  password: {value: 'password'}
};

const reduceFormFields = (form) => {
  return Object.keys(form).reduce((fields, fieldName) => ({...fields, [fieldName]: form[fieldName].value }), {});
};

describe('InfoController', () => {
  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      loadUser: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      updateFormDirtyState: sinon.spy(),
      editAccount: sinon.spy()
    };
    context = {
      router: { goBack: sinon.spy() },
      username: 'admin'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<InfoController {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should load current user', () => {
    shallow(<InfoController {...minimalProps}/>, {context});
    expect(minimalProps.loadUser).to.be.calledWith({userName: context.username});
  });

  describe('#submit', () => {
    let wrapper;
    let instance;
    let clock;
    let fakeNow;
    beforeEach(() => {
      wrapper = shallow(<InfoController {...commonProps}/>, {context});
      instance = wrapper.instance();
      sinon.stub(ApiUtils, 'attachFormSubmitHandlers').returns({ then: f => f()});
      fakeNow = new Date().getTime();
      clock = sinon.useFakeTimers(fakeNow);
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
      clock.restore();
    });

    it('should call editAccount from props with appropriate value on submit', () => {
      const expectedBody = {...reduceFormFields(commonFormValues), createdAt: fakeNow};
      instance.submit(commonFormValues);
      expect(commonProps.editAccount).to.be.calledWith(expectedBody, 'admin');
    });

    it('should reset form dirty state after submit', () => {
      instance.submit(commonFormValues);
      expect(commonProps.updateFormDirtyState).to.be.called;
    });
  });

  describe('#cancel', () => {
    it('should navigate user to previous route after click on canel button', () => {
      const instance = shallow(<InfoController {...minimalProps}/>, {context}).instance();
      instance.cancel();
      expect(context.router.goBack).to.be.called;
    });
  });
});
