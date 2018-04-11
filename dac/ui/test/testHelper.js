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
/* eslint react/prop-types: 0 */

import './commonGlobalVariables';
import { Component } from 'react';
import en from 'locales/en.json';
import 'url-search-params-polyfill';

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

window.URL.createObjectURL = window.URL.createObjectURL || function() {
  return 'blob:fake';
};

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
window.SVGPathElement = global.SVGPathElement = function() {};
global.SVGPathSegList = function() {};

class FakeComponent extends Component {
  render() {
    return <span>fake</span>;
  }
}

const checkIntlId = id => {
  if (!en[id]) throw new Error(`Intl id "${id}" does not exist.`);
};

const Module = require('module');
const originalRequire = Module.prototype.require;
Module.prototype.require = function(module) {
  // provide a fake react-intl lib:
  // - make it so that IntlProvider isn't required
  // - make it so that our tests can check output strings without hardcoding to English (as the English can change for design reasons)
  // WARNING: this is partial - grow it as you use more APIs
  if (module === 'react-intl') {
    return {
      FormattedMessage(...props) {
        if (!props.id) throw new Error('Missing `id` for FormattedMessage.');
        checkIntlId(props.id);
        return <span>{JSON.stringify(props)}</span>;
      },
      injectIntl(Wrapped) {
        // hack in the intl props in a way that keeps the tests simple
        Wrapped.defaultProps = Wrapped.defaultProps || {};
        Wrapped.defaultProps.intl = {
          formatMessage(desc) {
            if (!desc.id) throw new Error('Missing `id` for formatMessage.');
            checkIntlId(desc.id);
            return JSON.stringify(arguments);
          }
        };
        return Wrapped;
      }
    };
  }

  // since we are not in webpack, make glob-loader
  // work. (this is only tested for use with images/)
  const globLoadFile = (module.match(/^glob-loader!(.*)/) || [])[1];
  if (globLoadFile) {
    const patternFile = require.resolve(globLoadFile);
    const pattern = require('fs').readFileSync(patternFile, 'utf8'); // eslint-disable-line no-sync
    const relativeTo = patternFile.split('/').slice(0, -1).join('/');
    const files = require('glob').sync(relativeTo + '/' + pattern);

    const fileFakes = {};
    for (const file of files) {
      fileFakes['.' + file.slice(relativeTo.length)] = {default: FakeComponent};
    }

    return fileFakes;
  }

  return originalRequire.apply(this, arguments);
};

// webpack usually does this:
// (needed for monaco sql - and only built for that for now!!)
global.define = function(name, deps, func) {
  const out = {};
  func(require, out);
};
