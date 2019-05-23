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
import IntercomUtilsMixin from 'dyn-load/utils/intercomUtilsMixin';
import userUtils from 'utils/userUtils';
import localStorageUtils from './storageUtils/localStorageUtils';
import config from './config';

// DX-16408
const testEmails = ['@dremio.com', '@dremio.test', '@test.com'];
const testEmailRegex = new RegExp(`(${testEmails
  // escape special characters for regular expression
  .map(email => email.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
  .join('|')})`, 'i');

// export for testing
export const useTestIntercomApp = (userEmail) => !config.isReleaseBuild || testEmailRegex.test(userEmail);

const testIntercomAppId = 'z8apq4co';

// see https://docs.intercom.com/install-on-your-product-or-site/other-ways-to-get-started/integrate-intercom-in-a-single-page-app
// https://developers.intercom.com/v2.0/docs/intercom-javascript
@IntercomUtilsMixin
class IntercomUtils {

  ifChatAllowed() {
    return this._ifAllowed(true);
  }

  _ifAllowed(forChat = false) {
    const Intercom = global.Intercom;
    if (!Intercom) {
      return Promise.reject(new Error('INTERCOM MISSING'));
    }

    const userData = localStorageUtils.getUserData();

    if (!localStorage.getItem('isE2E') && Intercom && !config.outsideCommunicationDisabled) {
      if (userUtils.isAuthenticated(userData)) {
        // connect to intercom for other intercom features even if chat is disabled
        if (!forChat || this._shouldAllowChatForUser(userData)) {
          return Promise.resolve(Intercom);
        }
      }
    }
    return Promise.reject(new Error('INTERCOM NOT ALLOWED'));
  }

  // silently does nothing if Intercom in not available
  _sendToIntercom() {
    this._ifAllowed().then((Intercom) => {
      Intercom(...arguments);
    }, function() {
      console.error('Intercom communication error', ...arguments);
    }); // make non-fatal
  }

  // the following are pre-bound because it is common to pass the fcns around...

  boot = () => {
    this._ifAllowed().then((Intercom) => {
      const userData = localStorageUtils.getUserData();
      if (userData) {
        const email = userData.email;
        const appId = useTestIntercomApp(email) ? testIntercomAppId :
          (config.intercomAppId || testIntercomAppId); // if server does not provide app id, use a test one

        Intercom('boot', {
          'app_id': appId,
          email,
          'user_id': userData.clusterId + (email || userData.userId),
          'created_at': userData.userCreatedAt / 1000,
          'name': `${userData.firstName} ${userData.lastName}`.trim(),
          'company': {
            'id': userData.clusterId,
            'version': userData.version,
            'created_at': userData.clusterCreatedAt / 1000
          },
          widget: {
            activator: '#header-chat-button'
          },
          ...this._getExtraBootData()
        });
      }
    }, () => {}); // make non-fatal
  }

  // need so Intercom can keep track of where user is for single-page-app nav
  update = () => {
    // You can call Intercom('update') without getting throttled up to 10 times per page refresh.
    // After the 10th call, you'll be throttled and you'll be allowed to call Intercom('update')
    // a maximum of once every 30 minutes. Reloading the page will refresh this state.
    // TODO: look into `trackEvent` instead
    this._sendToIntercom('update');

    // todo: how do we avoid chat inbound from Intercom? (do we want to?)
  }

  shutdown = () => {
    this._sendToIntercom('shutdown');
  }
}

export default new IntercomUtils();
