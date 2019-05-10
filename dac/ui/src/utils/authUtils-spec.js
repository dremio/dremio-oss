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
import { isAuthorized, Capabilities } from './authUtils';

describe('authUtils (EE)', () => {
  //  it('getAuthInfoSelector and authInfoPropType are consistent', () => {
  //     const defaultStoreState = reducer(undefined, {}); // get default state // here is an error
  //     const propName = 'authInfo';
  //     const props = {
  //       [propName]: getAuthInfoSelector(state)
  //     };

  //     return PropTypes.checkPropTypes(authInfoPropType, props, propName, 'authUtils-spec'); // reuse stnadard prop types check
  //  });

  describe('isAuthorized', () => {
    it('An admin passes a check for admin pages', () => {
      const userAuthInfo = {
        isAdmin: true
      };
      const rule = {
        isAdmin: true
      };
      expect(isAuthorized(rule, userAuthInfo)).to.be.true;
    });

    it('An admin does not pass a check for non admin pages', () => {
      const userAuthInfo = {
        isAdmin: true
      };
      const rule = {
        isAdmin: false
      };
      expect(isAuthorized(rule, userAuthInfo)).to.be.false;
    });

    it('An non admin does not pass a check for admin pages', () => {
      const userAuthInfo = {
        isAdmin: false
      };
      const rule = {
        isAdmin: true
      };
      expect(isAuthorized(rule, userAuthInfo)).to.be.false;
    });

    it('An non admin does not have access to Space list if respective option is disabled', () => {
      const userAuthInfo = {
        isAdmin: false,
        allowSpaceManagement: false
      };
      const rule = {
        capabilities: [Capabilities.manageSpaces]
      };
      expect(isAuthorized(rule, userAuthInfo)).to.be.false;
    });

    it('An non admin has access to Space list if respective option is enabled', () => {
      const userAuthInfo = {
        isAdmin: false,
        allowSpaceManagement: true
      };
      const rule = {
        capabilities: [Capabilities.manageSpaces]
      };
      expect(isAuthorized(rule, userAuthInfo)).to.be.true;
    });
  });
});
