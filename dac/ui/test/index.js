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
/*eslint no-sync: 0*/

//https://github.com/sinonjs/fake-timers/issues/394#issuecomment-1021665619
global.Performance =
  global.Performance ||
  class Perf {
    clearMarks() {
      return global.performance.clearMarks();
    }
    clearMeasures() {
      return global.performance.clearMeasures();
    }
    clearResourceTimings() {
      return global.performance.clearResourceTimings();
    }
    getEntries() {
      return global.performance.getEntries();
    }
    getEntriesByName() {
      return global.performance.getEntriesByName();
    }
    getEntriesByType() {
      return global.performance.getEntriesByType();
    }
    mark() {
      return global.performance.mark();
    }
    measure() {
      return global.performance.measure();
    }
    now() {
      return global.performance.now();
    }
    setResourceTimingBufferSize() {
      return global.performance.setResourceTimingBufferSize();
    }
    toJSON() {
      return global.performance.toJSON();
    }
  };

// note: mocha watch doesn't seem to work outside of the cwd, so we have to play a few games (see package.json)

const path = require("path");
const dynLoader = require("../dynLoader");
const { InjectionResolver } = require("../scripts/injectionResolver");

// make sure babel works, even for dynamically loaded files
// alt: could probably move .babelrc, node_modules to dremio root
require("@babel/register")({
  configFile: path.join(__dirname, "..", ".babelrc.js"),
  extensions: [".js", ".jsx", ".ts", ".tsx"],
  ignore: [
    function (path) {
      if (!path.includes("/node_modules/")) {
        return false;
      }

      return !path.includes("/node_modules/parse-ms/");
    },
  ],
});

require("app-module-path").addPath(__dirname);
require("app-module-path").addPath(path.resolve(__dirname, "..", "src"));

dynLoader.applyNodeResolver();

require("./testHelper");

const injectionResolver = new InjectionResolver();
injectionResolver.applyNodeResolver();
