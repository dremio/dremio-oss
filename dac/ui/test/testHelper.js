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
import './commonGlobalVariables';

// Prevent compiling of .less
require.extensions['.less'] = () => {};
require.extensions['.css'] = () => {};
require.extensions['.svg'] = () => {};

// Define user agent for Radium
global.navigator = {
  userAgent: `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5)
              AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.97 Safari/537.36`
};

/* init jsdom  */

import { jsdom } from 'jsdom';

const exposedProperties = ['navigator', 'document'];

global.document = jsdom('');
global.window = document.defaultView;

global.window.config = { // will this it's FE-DEV, BE-DEV
  language: 'dataEN'
};

// jsdom has not getSelection https://github.com/tmpvar/jsdom/issues/321
// https://github.com/tmpvar/jsdom/issues/317
window.getSelection = function() {
  return {
    removeAllRanges: sinon.spy()
  };
};

window.requestAnimationFrame = setTimeout;
window.cancelAnimationFrame = clearTimeout;

Object.keys(document.defaultView).forEach((property) => {
  if (typeof global[property] === 'undefined') {
    exposedProperties.push(property);
    global[property] = document.defaultView[property];
  }
});

global.navigator = {
  userAgent: 'node.js'
};
global.localStorage = (() => {
  const store = {};
  const getItem = (key) => {
    return store[key];
  };
  const setItem = (key, value) => {
    store[key] = value;
  };
  const removeItem = (key) => {
    delete store[key];
  };
  return {
    getItem,
    setItem,
    removeItem
  };
})();

global.SVGPathSeg = function() {};
global.SVGPathSegClosePath = function() {};
global.SVGPathSegMovetoAbs = function() {};
global.SVGPathSegMovetoRel = function() {};
global.SVGPathSegLinetoAbs = function() {};
global.SVGPathSegLinetoRel = function() {};
global.SVGPathSegCurvetoCubicAbs = function() {};
global.SVGPathSegCurvetoCubicRel = function() {};
global.SVGPathSegCurvetoQuadraticAbs = function() {};
global.SVGPathSegCurvetoQuadraticRel = function() {};
global.SVGPathSegCurvetoQuadraticRel = function() {};
global.SVGPathSegArcAbs = function() {};
global.SVGPathSegArcRel = function() {};
global.SVGPathSegLinetoHorizontalAbs = function() {};
global.SVGPathSegLinetoHorizontalRel = function() {};
global.SVGPathSegLinetoVerticalAbs = function() {};
global.SVGPathSegLinetoVerticalRel = function() {};
global.SVGPathSegCurvetoCubicSmoothAbs = function() {};
global.SVGPathSegCurvetoCubicSmoothRel = function() {};
global.SVGPathSegCurvetoQuadraticSmoothAbs = function() {};
global.SVGPathSegCurvetoQuadraticSmoothRel = function() {};
global.SVGPathElement = function() {};
global.SVGPathSegList = function() {};
