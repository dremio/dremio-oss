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
/**
 * List of provision managers. Each item contains following info:
 *
 * @property {String} type think as `id`
 * @property {String} label displayed manager name
 * @property {Boolean} connected when true manager is active and can be configured
 * @property {Array} propsAsFields list of fields that picked from provision's entity
 * `subPropertyList` attribute that should be displayed as separate field. For example Yarn's
 * form has 2 special fields that taken from `subPropertyList` which are 'yarn.resourcemanager.host'
 * and 'fs.defaultFS'.
 *
 * @type {Array}
 */
export const PROVISION_MANAGERS = [ // todo: loc
  {
    clusterType: 'YARN',
    label: 'YARN',
    iconType: 'Hadoop',
    connected: true,
    propsAsFields: [
      {
        key: 'yarn.resourcemanager.hostname',
        field: 'resourceManagerHost'
      },
      {
        key: 'fs.defaultFS',
        field: 'namenodeHost'
      },
      {
        key: 'paths.spilling',
        field: 'spillDirectories'
      }
    ]
  }
  // , Temporary removed from the UI per DX-12716
  // {
  //   clusterType: 'MESOS',
  //   label: 'Mesos',
  //   iconType: 'Mesos',
  //   connected: false
  // },
  // {
  //   clusterType: 'KUBERNETES',
  //   label: 'Kubernetes',
  //   iconType: 'Kubernetes',
  //   connected: false
  // },
  // {
  //   clusterType: 'AMAZON',
  //   label: 'Amazon EC2',
  //   iconType: 'Amazon',
  //   connected: false
  // },
  // {
  //   clusterType: 'GCE',
  //   label: 'Google Cloud Platform',
  //   iconType: 'GCE',
  //   connected: false
  // },
  // {
  //   clusterType: 'AZURE',
  //   label: 'Microsoft Azure',
  //   iconType: 'Azure',
  //   connected: false
  // }
];
