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
/*eslint no-sync: 0*/

const path = require('path');

exports.path = path.resolve(__dirname, process.env.DREMIO_DYN_LOADER_PATH || './src');

// make the dyn-load module actually resolve, dynamically for mocha/node
// Note: this will not work with `require.resolve()`, but we generally use `import` anyway
exports.applyNodeResolver = () => {
  const Module = require('module');
  const originalRequire = Module.prototype.require;
  Module.prototype.require = function(module) {
    if (module.match(/^dyn-load($|\/)/)) {
      return originalRequire.call(this, module.replace(/^dyn-load/, exports.path));
    }
    return originalRequire.apply(this, arguments);
  };

  // also make it so that things in the dyn-load module can resolve things in our node_modules
  // alt: could probably move node_modules to dremio root
  exports.applyNodeModulesResolver();
};

// used by webpack/babel to resolve the babel plugins
exports.applyNodeModulesResolver = () => {
  require('app-module-path').addPath(path.resolve(__dirname, 'node_modules'));
};
