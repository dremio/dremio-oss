/* eslint-disable comma-dangle */
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
import {Server, Response} from 'miragejs';

//======
// use this file for api mocking while debugging the UI by adding a line to index.js:
// import MirageServer from 'dyn-load/MirageServer'; // eslint-disable-line no-unused-vars
//======

export const MirageServer = new Server({
  baseConfig() {
    this.urlPrefix = 'http://localhost:3005';
    // this.namespace = 'apiv2';

    this.get('apiv2/login', () => {
      return {
        'token': '1nf15tk90d3ca0jslobir29c0m',
        'userName': 'dremio',
        'firstName': 'dremio',
        'lastName': 'dremio',
        'expires': 1590036969647,
        'email': 'dremio@dremio.com',
        'userId': 'a4e67ebd-cf35-4360-804c-936a3d1737a8',
        'admin': true,
        'clusterId': '7532a686-4b8f-471f-829f-b1104fceee30',
        'clusterCreatedAt': 1589823242678,
        'showUserAndUserProperties': true,
        'version': '4.5.0-SNAPSHOT',
        'permissions': {
          'canUploadProfiles': true,
          'canDownloadProfiles': true,
          'canEmailForSupport': true,
          'canChatForSupport': false
        },
        'userCreatedAt': 1589823501207
      };
    });

    this.post('TODO', () => {
      return new Response(409, {some: 'header'}, {error: 'Invalid ...'});
    });
  }
});
