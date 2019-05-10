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
const noop = () => {};
let log = noop;
if (process.env.NODE_ENV !== 'production') {
  log = (...args) => {
    /* the method is based on chrome format. And works well only for chrome.
        Here is an format example:
        ---
        Error
        at Object.logger.log (webpack-internal:///3517:33:17)
        at {actual caller}$ (webpack-internal:///1429:287:26)
        ...
        ---
    */
    const stack = new Error().stack;
    let caller = stack.split('\n')[2].trim(); // at {actual caller}$ (webpack-internal:///1429:287:26)
    if (caller) {
      const prefix = 'at ';
      const startIndex = caller.indexOf(prefix) + prefix.length;
      const endIndex = caller.indexOf('$');
      caller = caller.substr(startIndex, endIndex - startIndex);
    }
    console.log(`${caller}:`, ...args); // eslint-disable-line no-console
  };
}

export {
  log
};
