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
const path = require('path');
const forEachBail = require('enhanced-resolve/lib/forEachBail');

const stubModule = path.resolve(__dirname, './stubModule.js');
const alias = '@inject/';

// optional path that allows controlling where injection happens - used by the dev server
const injectionPath = process.env.DREMIO_INJECTION_PATH ? path.resolve(__dirname, process.env.DREMIO_INJECTION_PATH) : null;

/**
 * A resolver that allow to check module location in different folder and fallback to a default module
 *
 *  Inspired by ./node_modules/enhanced-resolve/lib/ModulesInHierachicDirectoriesPlugin.js and
 *  ./node_modules/enhanced-resolve/lib/AliasPlugin.js
 */
class InjectionResolver {
  constructor(source, target) {
    this.source = source || 'resolve';
    this.target = target || 'resolve';
  }

  getPathsToCheck(originalPath) {
    const relativePath = originalPath.substring(alias.length);

    const pathsToCheck = [];

    if (injectionPath) {
      pathsToCheck.push(this.createInfo(originalPath, `${injectionPath}/${relativePath}`));
    }

    pathsToCheck.push(this.createInfo(originalPath, `dyn-load/${relativePath}`));
    pathsToCheck.push(this.createInfo(originalPath, `@app/${relativePath}`));
    // As last resort, go to the stub module that does nothing.
    pathsToCheck.push(this.createInfo(originalPath, stubModule, `'${originalPath}' is redirected to stub module: ${stubModule}`));

    return pathsToCheck;
  }

  apply(resolver) {
    const target = resolver.ensureHook(this.target);

    resolver.getHook(this.source).tapAsync('DremioResolver', (request, resolveContext, outerCallback) => {
      const innerRequest = request.request || request.path;
      if (!innerRequest) {
        return outerCallback();
      }

      // if the request starts with our alias, take over
      if (innerRequest.startsWith(alias)) {
        const locations = this.getPathsToCheck(innerRequest);

        forEachBail(locations, (info, callback) => {
          const { newPath, message } = info;
          const newRequest = {
            ...request,
            request: newPath
          };

          return resolver.doResolve(target, newRequest, message, resolveContext, callback);
        }, outerCallback);
        return;
      }
      return outerCallback();
    });
  }

  createInfo(originalPath, redirectToModulePath, message) {
    if (message === undefined) {
      message = `'${originalPath}' is redirected to module: ${redirectToModulePath}`;
    }
    return ({
      newPath: redirectToModulePath,
      message
    });
  }

  applyNodeResolver() {
    const Module = require('module');
    const originalRequire = Module.prototype.require;
    Module.prototype.require = function(module) {
      if (module.startsWith(alias)) {
        try {
          return originalRequire.call(this, module.replace(alias, '@dyn-load/'));
        } catch (e) {
          // ignored
        }

        try {
          return originalRequire.call(this, module.replace(alias, '@app/'));
        } catch (e) {
          // ignored
        }

        return originalRequire.call(this, stubModule);
      }
      return originalRequire.apply(this, arguments);
    };
  }
}

module.exports = {
  injectionPath,
  InjectionResolver
};
