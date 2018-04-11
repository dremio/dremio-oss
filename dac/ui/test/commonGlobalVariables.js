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
import enableFatalPropTypes from 'enableFatalPropTypes';
enableFatalPropTypes();

// make expect and sinon global
import 'polyfills';
import 'babel-polyfill';
import 'regeneratorRuntimeDefault';
import 'isomorphic-fetch';

import chai, { assert, expect } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import chaiString from 'chai-string';
import sinonChai from 'sinon-chai';
import chaiImmutable from 'chai-immutable';
import sinon from 'sinon';
import Immutable from 'immutable';
import React from 'react';
import { WebSocket } from 'mock-socket';

chai.use(chaiAsPromised);
chai.use(chaiString);
chai.use(chaiImmutable);
chai.use(sinonChai);

global.assert = assert;
global.expect = expect;
global.sinon = sinon;
global.Immutable = Immutable;
global.React = React;
global.WebSocket = WebSocket;
// i18n function
global.la = (text) => text;
