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
import { call, takeLatest } from 'redux-saga/effects';
import {
  afterLogin, handleLogin, handleAppInit,
  checkAppState, resetAppInitState, handleAppStop
} from '@app/sagas/loginLogout';
import { LOGIN_USER_SUCCESS } from '@app/actions/account';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import { expect } from 'chai';
import { default as handleAppInitHelper } from '@inject/sagas/utils/handleAppInit';
import { isAuthorized } from '@inject/sagas/utils/isAuthorized';

describe('login', () => {
  it('afterLogin calls handleLogin saga if LOGIN_USER_SUCCESS is dispatched', () => {
    const gen = afterLogin();
    expect(gen.next().value).to.be.eql(takeLatest(LOGIN_USER_SUCCESS, handleLogin));
    expect(gen.next().done).to.be.true;
  });

  it('handleLogin sets user data, boots an app and go to home page', () => {
    const userData = { name: 'a test user name' };
    const gen = handleLogin({ payload: userData });

    expect(gen.next().value).to.be.eql(call([localStorageUtils, localStorageUtils.setUserData], userData));
    expect(gen.next().value).to.be.eql(call(handleAppInit));

    expect(gen.next().done).to.be.true;
  });

  describe('handleAppBoot', () => {
    let gen;

    beforeEach(() => {
      resetAppInitState();
    });

    const testBoot = () => {
      gen = handleAppInit();
      expect(gen.next().value).to.be.eql(call(handleAppInitHelper));
      expect(gen.next().done).to.be.true;
    };

    it('has effect only once until handleAppStop is called', () => {
      testBoot();
      // call app init several times. Saga should be completed immediately
      expect(handleAppInit().next().done).to.be.true;
      expect(handleAppInit().next().done).to.be.true;
      const stopAppGen = handleAppStop();
      while (!stopAppGen.next().done) { // execute full stopAppGen saga
        // noop
      }
      // standard flow should be enabled
      testBoot();
    });
  });

  describe('checkAppState', () => {
    let gen = null;

    beforeEach(() => {
      gen = checkAppState();
      expect(gen.next().value).to.be.eql(call(isAuthorized));
    });

    afterEach(() => {
      expect(gen.next().done).to.be.true;
    });

    it('clears current user data is a user in not authorized anymore', () => {
      expect(gen.next(false).value).to.be
        .eql(call([localStorageUtils, localStorageUtils.clearUserData]));
    });
    it('runs handleAppBoot if user is valid', () => {
      expect(gen.next(true).value).to.be.eql(call(handleAppInit));
    });
  });
});
