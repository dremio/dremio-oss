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

import moize from "moize";
import { waitForNetworkAvailable } from "./useNetworkAvailable";

const CHECK_INTERVAL = 1500;

/**
 * Check if the UI application server (origin) is reachable without
 * a fetch error. The response should be memoized so that if this
 * function gets called in multiple places we only make one request at a time.
 */
const canReachOrigin = moize.promise(
  () =>
    fetch(window.location.origin)
      .then(() => true)
      .catch(() => false),
  { maxAge: CHECK_INTERVAL / 2 }
);

/**
 * In some cases the network may time out in which case it can be
 * better to retry the request rather than continuing to wait.
 */
const failAfterTimeout = (timeout: number) =>
  new Promise((resolve, reject) => setTimeout(() => reject(), timeout))
    .then(() => true)
    .catch(() => false);

export const isServerReachable = (): Promise<boolean> =>
  Promise.race([canReachOrigin(), failAfterTimeout(10000)]);

/**
 * Returns a promise that resolves once connectivity is restored
 */
export const waitForServerReachable = (): Promise<void> => {
  return new Promise((resolve) => {
    const run = () => {
      setTimeout(async () => {
        await waitForNetworkAvailable();
        if (await isServerReachable()) {
          resolve();
        } else {
          run();
        }
      }, CHECK_INTERVAL);
    };
    run();
  });
};
