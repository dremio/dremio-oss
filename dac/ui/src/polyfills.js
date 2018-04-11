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
import platform from 'platform';

// Best way to force Promise Polyfill usage
// so that we get window.onunhandledrejection.
// related: https://github.com/getsentry/raven-js/issues/424#issuecomment-181928614

delete global.Promise;

// Node.prototype.parentElement polyfill for IE11
if (global.Node && !('parentElement' in global.Node.prototype)) {
  Object.defineProperty(Node.prototype, 'parentElement', {
    configurable: true,
    enumerable: true,
    get() {
      const parent = this.parentNode;
      if (!parent || !(parent instanceof Element)) {
        return null;
      }
      return parent;
    }
  });
}

// use fetch polyfill for MS Edge because of few issues:
// - 401 error status doesn't respond with correct object in .catch() block
//   https://developer.microsoft.com/en-us/microsoft-edge/platform/issues/8653298/
// - utf chars in request body cause silent failure
//   https://developer.microsoft.com/en-us/microsoft-edge/platform/issues/8475223/
if (platform.name === 'Microsoft Edge') {
  delete global.fetch;
}
