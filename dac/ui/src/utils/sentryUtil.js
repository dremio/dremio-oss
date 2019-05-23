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
import Raven from 'raven-js';
import uuid from 'uuid';
import { getVersionWithEdition } from 'dyn-load/utils/versionUtils';
import config from './config';

/*
  !!! Important. You must verify that this utils send logs to sentry correctly in production
  release mode (i.e. NODE_ENV='production' and DREMIO_RELEASE=true). I saw the cases when logs were
  sent correctly in case DREMIO_RELEASE=false, but was not sent in case DREMIO_RELEASE=true.
  (See ErrorBoundary for example)
 */
class SentryUtil {

  // we'd really like to have a uuid for the error, but cross-referencing sentry with the UI
  // will be prone to timing issues. So just creating a session UUID that we can use - it should be
  // good enough to find records given a report.
  sessionUUID = uuid.v4();

  install() {
    if (config.isReleaseBuild && !config.outsideCommunicationDisabled) {
      Raven.config('https://2592b22bfefa49b3b5b1e72393f84194@sentry.io/66750', {
        release: getVersionWithEdition(),
        serverName: config.clusterId,
        // extra info that could be used to search an error.
        // example: sessionUUID:"1ac6a0bb-6582-4532-81c3-5b2ac479dcab"
        tags: {
          sessionUUID: this.sessionUUID,
          commitHash: config.versionInfo.commitHash
        }
      }).install();
    }
  }

  logException(ex, context) {
    if (config.isReleaseBuild && !config.outsideCommunicationDisabled) {
      Raven.captureException(ex, {
        extra: context
      });
      global.console && console.error && console.error(ex, context);
    }
  }

  getEventId() {
    return Raven.lastEventId();
  }
}

export default new SentryUtil();
