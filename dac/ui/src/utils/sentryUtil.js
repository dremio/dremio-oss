/*
 * Copyright (C) 2017 Dremio Corporation
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
import Raven from 'raven-js';
import config, { isProductionServer } from './config';

class SentryUtil {

  install() {
    if (isProductionServer() && !config.outsideCommunicationDisabled) {
      Raven.config('https://2592b22bfefa49b3b5b1e72393f84194@app.getsentry.com/66750').install();
      Raven.setUserContext({
        email: config.email // todo: this isn't set up by anything?
      });
    }
  }

  logException(ex, context) {
    if (isProductionServer() && !config.outsideCommunicationDisabled) {
      Raven.captureException(ex, {
        extra: context
      });
      global.console && console.error && console.error(ex, context);
    }
  }
}

export default new SentryUtil();
