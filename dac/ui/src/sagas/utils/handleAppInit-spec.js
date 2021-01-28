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
import { call, put, select } from 'redux-saga/effects';
import { replace } from 'react-router-redux';
import { getLocation } from '@app/selectors/routing';
import {
  LOGIN_PATH, SIGNUP_PATH, SSO_LANDING_PATH
} from '@app/sagas/loginLogout';
import { expect } from 'chai';
import handleAppInit from '@app/sagas/utils/handleAppInit';
import socket from '@app/utils/socket';

describe('handleAppInit', () => {
  let gen;

  const testBoot = () => {
    gen = handleAppInit();
    expect(gen.next().value).to.be.eql(call([socket, socket.open]));
    expect(gen.next().value).to.be.eql(select(getLocation));
  };

  const testRedirect = (fromUrl) => {
    const redirectUrl = 'some/url';
    expect(gen.next({
      pathname: fromUrl,
      query: { redirect: redirectUrl }
    }).value).to.be.eql(put(replace(redirectUrl)));

    expect(gen.next().done).to.be.true;

  };

  it('redirects from login page', () => {
    testBoot();
    testRedirect(LOGIN_PATH);
  });

  it('redirects from sign up page', () => {
    testBoot();
    testRedirect(SIGNUP_PATH);
  });

  it('redirects from social login page', () => {
    testBoot();
    testRedirect(SSO_LANDING_PATH);
  });

  it('does not redirect form other pages', () => {
    testBoot();
    const redirectUrl = 'some/url';
    expect(gen.next({
      pathname: '/home',
      query: { redirect: redirectUrl }
    }).done).to.be.true;
  });
});
