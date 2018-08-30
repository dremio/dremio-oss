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
import { replace } from 'react-router-redux';
import { UserAuthWrapper } from 'redux-auth-wrapper';
import userUtils from 'utils/userUtils';

export const UserIsAuthenticated = UserAuthWrapper({
  authSelector: state => state.account.get('user'),
  predicate: userUtils.isAuthenticated,
  redirectAction: replace,
  wrapperDisplayName: 'UserIsAuthenticated'
});

export const UserIsAdmin = function() {
  const isAdmin = UserAuthWrapper({
    authSelector: state => state.account.get('user'),
    predicate: userUtils.isAdmin,
    redirectAction: replace,
    failureRedirectPath: '/',
    wrapperDisplayName: 'UserIsAdmin',
    // In UserIsAuthenticated we should allow to redirect to some page from login page.
    // But if already some authenticated user will try access an admin page it will
    // redirect him to / and then he won't be able to redirect back the to admin page.
    allowRedirectBack: false
  });

  return isAdmin(UserIsAuthenticated(...arguments));
};
