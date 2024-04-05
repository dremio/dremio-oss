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
import * as Sentry from "@sentry/browser";
import { v4 as uuidv4 } from "uuid";
import { getVersion } from "@root/scripts/versionUtils";
import config from "./config";

/*
  !!! Important. You must verify that this utils send logs to sentry correctly in production
  release mode (i.e. NODE_ENV='production' and DREMIO_RELEASE=true). I saw the cases when logs were
  sent correctly in case DREMIO_RELEASE=false, but was not sent in case DREMIO_RELEASE=true.
  (See ErrorBoundary for example)
 */

const SENTRY_URL = new URL(
  "https://2592b22bfefa49b3b5b1e72393f84194@o31066.ingest.sentry.io/66750",
);

const sentryIsReachable = () => {
  return fetch(SENTRY_URL.origin, { mode: "no-cors" })
    .then(() => true)
    .catch(() => false);
};
class SentryUtil {
  // we'd really like to have a uuid for the error, but cross-referencing sentry with the UI
  // will be prone to timing issues. So just creating a session UUID that we can use - it should be
  // good enough to find records given a report.
  sessionUUID = uuidv4();
  isReachable = null;

  async install() {
    if (config.logErrorsToSentry && !config.outsideCommunicationDisabled) {
      this.isReachable = await sentryIsReachable();
      if (!this.isReachable) {
        return;
      }
      Sentry.init({
        dsn: SENTRY_URL.toString(),
        release: getVersion(),
        serverName: config.clusterId,
      });

      // extra info that could be used to search an error.
      // example: sessionUUID:"1ac6a0bb-6582-4532-81c3-5b2ac479dcab"
      Sentry.setTags({
        sessionUUID: this.sessionUUID,
        commitHash: config.versionInfo.commitHash,
      });
    }
  }

  logException(ex, context) {
    if (
      config.logErrorsToSentry &&
      !config.outsideCommunicationDisabled &&
      this.isReachable
    ) {
      Sentry.withScope((scope) => {
        scope.setExtras(context);
        Sentry.captureException(ex);
      });
      global.console && console.error && console.error(ex, context);
    } else {
      return "failed";
    }
  }

  getEventId() {
    return Sentry.lastEventId();
  }
}

export default new SentryUtil();
