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
import { put, take } from 'redux-saga/effects';
import { replace } from 'react-router-redux';

import { fetchLoginUser, LOGIN_USER_SUCCESS } from 'actions/account';
import { handleSignup } from './signupUser';

describe('signupUser saga', () => {
  const form = {
    userName: 'u',
    password: 'p'
  };
  it('should not do anything when created user is not first one', () => {
    const gen = handleSignup({ meta: { form, isFirstUser: false}});
    expect(gen.next().value).to.be.undefined;
  });

  it('should login user if he is the one and redirect to home page', () => {
    const gen = handleSignup({ meta: { form, isFirstUser: true}});
    gen.next(); // getLocation
    expect(gen.next().value).to.eql(put(fetchLoginUser(form)));
    expect(gen.next().value).to.eql(take(LOGIN_USER_SUCCESS));
    expect(gen.next().value).to.eql(put(replace({pathname: '/'})));
  });
});
