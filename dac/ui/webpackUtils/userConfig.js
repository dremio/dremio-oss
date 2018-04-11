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

const fs = require('fs');
const path = require('path');

const read = () => {
  const userConfig = JSON.parse(fs.readFileSync(
    path.resolve(__dirname, '..', 'user.webpack.config.defaults.json'),
    'utf8'
  ));
  try {
    Object.assign(userConfig, JSON.parse(fs.readFileSync( // eslint-disable-line no-restricted-properties
      path.resolve(__dirname, '..', 'user.webpack.config.json'),
      'utf8'
    )));
  } catch (e) {
    // nada
  }
  return userConfig;
};

module.exports = read();

module.exports.live = read;
