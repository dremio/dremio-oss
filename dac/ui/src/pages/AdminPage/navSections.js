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

export default function(config) {
  return Promise.resolve([
    {
      title: la('Cluster'),
      items: [
        { name: la('Node Activity'), url: '/admin/nodeActivity' },
        { name: la('Provisioning'), url: '/admin/provisioning' },
        { name: la('Support'), url: '/admin/support' },
        { name: la('Queue Control'), url: '/admin/advanced' }
      ]
    },
    config.showUserAndUserProperties && {
      title: la('User Management'),
      items: [
        { name: la('Users'), url: '/admin/users' }
      ]
    }
  ].filter(Boolean));
}
