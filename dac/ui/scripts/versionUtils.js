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
const version = process.env.DREMIO_VERSION || 'DREMIO_VERSION_IS_NOT_PROVIDED';
const editionType = process.env.EDITION_TYPE || 'ce'; // community

// this module would be used in node environment in webpack.config.js and in babel environment in
// sentryUtils.js
module.exports = {
  getVersion() {
    return `${version}-${editionType}`;
  }
};
