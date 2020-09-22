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
// IE related polyfills. Test environment does not need it.

// this will cause an error in test environment, as window is not defined at the moment,
// when polyfills are imported
import 'element-closest';

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
