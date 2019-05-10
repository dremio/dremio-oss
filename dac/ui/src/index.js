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

import React from 'react';
import { render } from 'react-dom';
import $ from 'jquery';
import Immutable from 'immutable';

import './vendor/chat';
import './vendor/gtm';
import sentryUtil from 'utils/sentryUtil';
import startup from 'dyn-load/startup';
import metrics from './metrics';

import 'imports-loader?this=>window!script-loader!jsplumb/dist/js/jsPlumb-2.1.4-min.js';
import './main.less';
// add css here to be sure that its content will appear after compiled main.less content.
// when import .css file inside of .less file than .css content appears at the top of the file
// no matter of @import ordering
// the reason why typography.css import is here because it conflicts with reset.less imported inside
// of main.less file
import './uiTheme/css/typography.css';
import 'font-awesome/css/font-awesome.css';
import Root from './containers/Root';
import configureStore from './store/configureStore';

// useful debugging leaks...
window.React = React;
window.$ = $;
window.Immutable = Immutable;

window.DremioMetrics = metrics;

window.la = (key) => {
  // (config.environment !== 'PRODUCTION') && console.warn('using unsupported localization function for:', key);
  return key;
};

sentryUtil.install();

const store = configureStore();

startup.run();

render(
  <Root store={store} />,
  document.getElementById('root')
);
