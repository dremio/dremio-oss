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
import { LOGOUT_USER_START, LOGIN_USER_SUCCESS } from 'actions/account';
import { APP_BOOT } from 'actions/app';

import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import intercomUtils from 'utils/intercomUtils';
import socket from 'utils/socket';

import rootReducer from './index';

describe('rootReducer', () => {
  it('returns initial state from app reducers', () => {
    const result = rootReducer(undefined, {type: 'bla'});
    expect(result.confirmation.isOpen).to.be.false;
  });

  describe('LOGOUT_USER_START', () => {
    it('should reset everything except routing', () => {
      const initialState = rootReducer(undefined, {
        type: 'bla'
      });
      initialState.confirmation = {...initialState.confirmation, isOpen: true};
      initialState.routing.someAttribute = true;

      const result = rootReducer(initialState, {
        type: LOGOUT_USER_START
      });
      expect(result.confirmation.isOpen).to.be.false;
      expect(result.routing.someAttribute).to.be.true;
    });

    it('should call localStorageUtils.clearUserData(), intercomUtils.shutdown(), socket.close()', () => {
      sinon.stub(localStorageUtils, 'clearUserData');
      sinon.stub(intercomUtils, 'shutdown');
      sinon.stub(socket, 'close');
      rootReducer({}, {
        type: LOGOUT_USER_START
      });

      expect(localStorageUtils.clearUserData).to.be.called;
      expect(intercomUtils.shutdown).to.be.called;
      expect(socket.close).to.be.called;

      localStorageUtils.clearUserData.restore();
      intercomUtils.shutdown.restore();
      socket.close.restore();
    });
  });

  describe('user setup', () => {
    beforeEach(() => {
      sinon.stub(localStorageUtils, 'setUserData');
      sinon.stub(intercomUtils, 'boot');
      sinon.stub(socket, 'open');
    });
    afterEach(() => {
      localStorageUtils.setUserData.restore();
      intercomUtils.boot.restore();
      socket.open.restore();
    });
    it('LOGIN_USER_SUCCESS should store user data and prep intercom and socket', () => {
      rootReducer({}, {
        type: LOGIN_USER_SUCCESS,
        payload: {foo: 'bar'}
      });

      expect(localStorageUtils.setUserData).to.be.calledWith({foo: 'bar'});
      expect(intercomUtils.boot).to.be.called;
      expect(socket.open).to.be.called;
    });
    it('APP_BOOT should prep intercom and socket', () => {
      rootReducer({}, {
        type: APP_BOOT
      });

      expect(localStorageUtils.setUserData).to.not.be.called;
      expect(intercomUtils.boot).to.be.called;
      expect(socket.open).to.be.called;
    });
  });
});
