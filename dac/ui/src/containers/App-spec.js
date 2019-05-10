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

import NotificationContainer from 'containers/Notification';
import ConfirmationContainer from 'containers/Confirmation';
import ProdErrorContainer from 'containers/ProdError';


import { App } from './App';

describe('App-spec', () => {
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {

    commonProps = {
      user: {},
      location: {},
      params: {},
      dispatch: sinon.spy(),
      serverStatus: Immutable.Map(),
      children: 'test app content',
      shouldEnableRSOD: false
    };

    wrapper = shallow(<App {...commonProps}/>);
    instance = wrapper.instance();
  });

  describe('render', () => {
    it('should render containers', () => {
      expect(wrapper.find(NotificationContainer)).to.have.length(1);
      expect(wrapper.find(ConfirmationContainer)).to.have.length(1);
      expect(wrapper.find(ProdErrorContainer)).to.have.length(1);
    });
  });

  describe('#handleGlobalError()', () => {
    it('should ignore when url origin is different than window.location.origin', () => {
      sinon.stub(instance, 'displayError');
      sinon.stub(instance, '_getWindowOrigin').returns('http://localhost:4000');
      instance.handleGlobalError(undefined, 'message', 'https://foo');
      expect(instance.displayError).to.not.be.called;
      instance.handleGlobalError(undefined, 'message', instance._getWindowOrigin());
      expect(instance.displayError).to.be.called;
    });

    it('should ignore when url is ""', () => {
      sinon.stub(instance, 'displayError');
      instance.handleGlobalError(undefined, 'message', '');
      expect(instance.displayError).to.not.be.called;
    });

    it('should pass error || message to displayError', () => {
      sinon.stub(instance, 'displayError');
      sinon.stub(instance, '_getWindowOrigin').returns('http://localhost:4000');
      instance.handleGlobalError(undefined, 'message', instance._getWindowOrigin(), null, null, 'error');
      expect(instance.displayError).to.be.calledWith('error');
    });

    it('should call previous onerror if defined', () => {
      const prevOnerror = sinon.stub();
      instance.handleGlobalError(prevOnerror, 'message', 1, 2, 3, 'error');
      expect(prevOnerror).to.be.calledWith('message', 1, 2, 3, 'error');
    });
  });

  describe('#_shouldIgnoreExternalStack()', () => {
    it('returns true when stackOrigin is not current origin', () => {
      expect(instance._shouldIgnoreExternalStack()).to.be.false;
      expect(
        instance._shouldIgnoreExternalStack(`aaa\n at a (${window.location.origin}/foo)`)
      ).to.be.false;
      expect(
        instance._shouldIgnoreExternalStack('aaa\n at a (http://test.example.com:4000/foo)')
      ).to.be.true;
    });
  });

  describe('#_getSourceOriginFromStack()', () => {
    it('should return undefined if no stack or no match', () => {
      expect(instance._getSourceOriginFromStack()).to.be.undefined;
      expect(instance._getSourceOriginFromStack('')).to.be.undefined;
      expect(instance._getSourceOriginFromStack('aaa\nbbb')).to.be.undefined;
    });

    it('should return origin from stack', () => {
      // Chrome
      expect(
        instance._getSourceOriginFromStack('aaa\n at a (http://test.example.com:4000/foo)')
      ).to.equal('http://test.example.com:4000');

      // Firefox
      expect(
        instance._getSourceOriginFromStack(
          'ColumnHeader/_this.handleFocus/</<@http://localhost:3005/bundle.js line 12292 > eval:211:15\n' +
          'notify/</run@http://localhost:3005/bundle.js line 17434 > eval:87:22'
        )
      ).to.equal('http://localhost:3005');
    });
  });
});
