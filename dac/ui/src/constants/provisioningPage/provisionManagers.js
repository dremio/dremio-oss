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

//TODO: move fields and init_values to provisioningConstants
export const MAPPED_FIELDS = {
  resourceManagerHost: 'resourceManagerHost',
  namenodeHost: 'namenodeHost',
  nodeTag: 'nodeTag',
  spillDirectories: 'spillDirectories'
};

export const FIELDS = [
  'id', 'clusterType', 'queue',
  'memoryMB', 'virtualCoreCount', 'dynamicConfig.containerCount',
  'tag', 'distroType', 'isSecure',
  'propertyList[].name', // we map from entity.key -> field.name in mapToFormFields to match what Property input expects.
  'propertyList[].value',
  'propertyList[].type',
  MAPPED_FIELDS.resourceManagerHost,
  MAPPED_FIELDS.namenodeHost,
  MAPPED_FIELDS.nodeTag,
  MAPPED_FIELDS.spillDirectories + '[]'
];

export const INIT_VALUES = {};

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
        field: MAPPED_FIELDS.resourceManagerHost
      },
      {
        key: 'fs.defaultFS',
        field: MAPPED_FIELDS.namenodeHost
      },
      {
        key: 'services.node-tag',
        field: MAPPED_FIELDS.nodeTag
      },
      {
        key: 'paths.spilling',
        field: MAPPED_FIELDS.spillDirectories,
        isArray: true
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
  //   clusterType: 'EC2',
  //   label: 'AWS Cluster',
  //   iconType: 'Amazon',
  //   connected: true
  // }
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
