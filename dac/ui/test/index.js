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

// note: mocha watch doesn't seem to work outside of the cwd, so we have to play a few games (see package.json)

const fs = require('fs');
const path = require('path');
const dynLoader = require('../dynLoader');

// make sure babel works, even for dynamically loaded files
// alt: could probably move .babelrc, node_modules to dremio root
require('babel-register')(JSON.parse(fs.readFileSync(path.join(__dirname, '..', '.babelrc'), 'utf8')));

require('app-module-path').addPath(__dirname);
require('app-module-path').addPath(path.resolve(__dirname, '..', 'src'));

dynLoader.applyNodeResolver();

require('./testHelper');
