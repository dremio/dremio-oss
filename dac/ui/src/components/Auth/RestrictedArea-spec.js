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
import * as authUtils from '@app/utils/authUtils';
import { createStore } from 'redux';
import { Provider } from 'react-redux';
import { shallow, mount } from 'enzyme';
import { AuthorizationWrapperView, RestrictedArea } from './RestrictedArea';

describe('AuthorizationWrapperView', () => {
  const testValue = value => {
    try {
      const renderFn = sinon.stub().returns(<div></div>); // temp content
      sinon.stub(authUtils, 'isAuthorized').returns(value);
      const props = {
        authInfo: {}, // required property
        rule: {}, // required property
        children: renderFn
      };

      shallow(<AuthorizationWrapperView {...props} />);
      expect(renderFn.calledWith(true)).to.equal(value);
    } finally {
      authUtils.isAuthorized.restore();
    }
  };

  it('Render function is called with true', () => {
    testValue(true);
  });

  it('Render function is called with false', () => {
    testValue(false);
  });
});

describe('RestrictedArea', () => {
  const content = (<div>asdsad</div>);
  let minimalProps;
  let wrapper;

  const renderComponent = () => {
    const reducer = (state = {}) => state;
    const providerEl = mount(<Provider store={createStore(reducer)}>
      <RestrictedArea {...minimalProps} />
    </Provider>);

    wrapper = providerEl;//.childAt(1);
  };

  beforeEach(() => {
    minimalProps = {
      rule: {}, // required property
      children: content
    };

    sinon.stub(authUtils, 'getAuthInfoSelector').returns({});

  });

  afterEach(() => {
    authUtils.getAuthInfoSelector.restore();
  });

  it('Nothing is rendered if an user is not authorized', () => {
    try {
      sinon.stub(authUtils, 'isAuthorized').returns(false);
      renderComponent();
      expect(authUtils.isAuthorized.called).to.be.true;
      expect(wrapper.html()).to.equal(null); // nothing is rendered
    } finally {
      authUtils.isAuthorized.restore();
    }
  });

  it('Content is rendered if an user is authorized', () => {
    try {
      sinon.stub(authUtils, 'isAuthorized').returns(true);
      renderComponent();
      expect(wrapper.html()).to.equal(shallow(content).html());
    } finally {
      authUtils.isAuthorized.restore();
    }
  });
});
