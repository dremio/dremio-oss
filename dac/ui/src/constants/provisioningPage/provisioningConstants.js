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

import SourceProperties from '@app/components/Forms/SourceProperties';

export const CLUSTER_STATE = {
  running: 'RUNNING',
  stopped: 'STOPPED',
  starting: 'STARTING',
  stopping: 'STOPPING',
  deleted: 'DELETED',
  pending: 'PENDING',
  provisioning: 'PROVISIONING',
  failed: 'FAILED',
  online: 'ONLINE',
  offline: 'OFFLINE',
  unknown: 'UNKNOWN'
};

// labels used in nodes list
export const CLUSTER_STATE_LABEL = {
  running: 'Running',
  pending: 'Pending',
  provisioning: 'Provisioning or Disconnected',
  decommissioning: 'Decommissioning'
};

export const CLUSTER_STATE_ICON = {
  [CLUSTER_STATE.running]: {src: 'Online.svg', text: 'Online'},
  [CLUSTER_STATE.stopped]: {src: 'Disconnected.svg', text: 'Stopped'},
  [CLUSTER_STATE.starting]: {src: 'StartingEngine.svg', text: 'Starting'},
  [CLUSTER_STATE.stopping]: {src: 'Decommissioning.svg', text: 'Stopping'},
  [CLUSTER_STATE.deleted]: {src: 'Blocked.svg', text: 'Deleted'},
  [CLUSTER_STATE.pending]: {src: 'PendingState.svg', text: 'Pending'},
  [CLUSTER_STATE.provisioning]: {src: 'Provisioning.svg', text: 'Provisioning'},
  [CLUSTER_STATE.failed]: {src: 'Error.svg', text: 'Failed'},
  [CLUSTER_STATE.unknown]: {src: 'Trace.svg', text: 'Unknown'}
};

export const EC2_CLUSTER_FIELDS = ['name'];
export const EC2_UI_FIELDS = ['engineSize'];
export const EC2_DYNAMIC_CONFIG_FIELDS = ['containerCount'];
export const EC2_AWS_PROPS = [
  'vpc', 'nodeIamInstanceProfile', 'amiId', 'sshKeyName', 'securityGroupId', 'subnetId', 'instanceType',
  'extraConfProps', 'useClusterPlacementGroup', 'disablePublicIp'
];
export const EC2_AWS_PROPLIST_FIELDS = ['awsTags'];
export const EC2_AWS_CONNECTION_PROPS = [
  'authMode', 'accessKey', 'secretKey', 'endpoint', 'assumeRole'
];
export const EC2_FIELDS = [
  ...EC2_CLUSTER_FIELDS,
  ...EC2_UI_FIELDS,
  ...EC2_DYNAMIC_CONFIG_FIELDS,
  ...EC2_AWS_PROPS,
  ...SourceProperties.getFields({propName: EC2_AWS_PROPLIST_FIELDS[0]}),
  ...EC2_AWS_CONNECTION_PROPS];
export const EC2_FIELDS_MAP = EC2_FIELDS.reduce((a, field) => {
  a[field] = field;
  return a;
}, {});

export const DREMIO_CUSTOM_REGION = '$DREMIO_CUSTOM_ENDPOINT_URL$';

export const AWS_INSTANCE_TYPE_OPTIONS = [
  {label: 'Evaluation m5d.2xlarge (8c/32gb)', value: 'm5d.2xlarge'},
  {label: 'Standard m5d.8xlarge (32c/128gb)', value: 'm5d.8xlarge'},
  {label: 'High Memory r5d.4xlarge (16c/128gb)', value: 'r5d.4xlarge'},
  {label: 'High CPU c5d.18xlarge (72c/144gb)', value: 'c5d.18xlarge'},
  {label: 'High Cache i3.4xlarge (16c/122gb)', value: 'i3.4xlarge'}
];

export const ENGINE_SIZE = [
  {id: 'SMALL', label: 'Small - 2', value: 2},
  {id: 'MEDIUM', label: 'Medium - 4', value: 4},
  {id: 'LARGE', label: 'Large - 8', value: 8},
  {id: 'XLARGE', label: 'X Large - 16', value: 16},
  {id: '2XLARGE', label: '2X Large - 32', value: 32},
  {id: '3XLARGE', label: '3X Large - 64', value: 64},
  {id: 'CUSTOM', label: 'Custom', value: -1}
];

export const AWS_REGION_OPTIONS = [
  {label: 'US East (N. Virginia)', value: 'us-east-1'},
  {label: 'US West (N. California)', value: 'us-west-1'},
  {label: 'US West (Oregon)', value: 'us-west-2'},
  {label: 'EU (Ireland)', value: 'eu-west-1'},
  {label: 'Asia Pacific (Singapore)', value: 'ap-southeast-1'}
];

export const ENGINE_FILTER_NAME = {
  status: 'st',
  size: 'sz'
};

const stateOptions = Object.keys(CLUSTER_STATE_ICON).map(key => {
  return {id: key, label: CLUSTER_STATE_ICON[key].text};
});
export const ENGINE_FILTER_ITEMS = {
  [ENGINE_FILTER_NAME.status]: stateOptions.slice(0, stateOptions.length - 1),
  [ENGINE_FILTER_NAME.size]: ENGINE_SIZE
};


export const DEFAULT_ENGINE_FILTER_SELECTIONS = {
  [ENGINE_FILTER_NAME.status]: [],
  [ENGINE_FILTER_NAME.size]: []
};
export const ENGINE_FILTER_LABEL = {
  [ENGINE_FILTER_NAME.status]: 'Status',
  [ENGINE_FILTER_NAME.size]: 'Size'
};


export const ENGINE_COLUMNS_CONFIG = [
  {key: 'status', label: '', width: 25, flexGrow: 0, style: {marginRight: 3}},
  {key: 'engine', label: 'Engine', flexGrow: 1, headerStyle: {marginLeft: -6}},
  {key: 'size', label: 'Size', width: 90, headerStyle: {marginLeft: -6}}, //# of workers
  {key: 'cores', label: 'Cores per Executor', width: 130, headerStyle: {marginLeft: -6}},
  {key: 'memory', label: 'Memory per Executor', width: 130, headerStyle: {marginLeft: -6}},
  {key: 'ip', label: 'IP address', width: 120, headerStyle: {marginLeft: -6}},
  {key: 'nodes', label: 'Online Nodes', width: 100},
  {key: 'action', label: 'Action', width: 100, disableSort: true}
];

export const NODE_COLUMNS_CONFIG = [
  { key: 'status', label: 'Status', flexGrow: 2 },
  { key: 'host', label: 'Host', flexGrow: 4 },
  { key: 'memoryMB', label: 'Memory (MB)', align: 'right', width: 131 },
  { key: 'virtualCoreCount', label: 'Virtual Cores', align: 'right', width: 126 }
];

export const QUEUE_COLUMNS_CONFIG = [
  {key: 'name', label: 'Name', flexGrow: 2},
  {key: 'priority', label: 'CPU Priority', flexGrow: 1},
  {key: 'concurrency', label: 'Concurrency Limits', width: 140},
  {key: 'queueMemory', label: 'Queue Memory Limit', width: 140},
  {key: 'jobMemory', label: 'Job Memory Limit', width: 140}
];

export const ENGINE_SIZE_STANDARD_OPTIONS = [
  {value: 2, label: 'Small (2 nodes)', container: {}},
  {value: 4, label: 'Medium (4 nodes)', container: {}},
  {value: 8, label: 'Large (8 nodes)', container: {}},
  {value: 16, label: 'XLarge (16 nodes)', container: {}},
  {value: 32, label: '2XLarge (32 nodes)', container: {}},
  {value: 64, label: '3XLarge (64 nodes)', container: {}}
];

export const EC2_FORM_TAB_VLH = {
  sections: [
    {
      name: '',
      layout: 'row',
      elements: [
        {
          type: 'text',
          propName: 'name',
          propertyName: 'name',
          tooltip: 'A name for this engine. When editing a queue, you can specify the name of the engine to use for queries in that queue.',
          label: 'Engine Name',
          size: 446
        },
        {
          type: 'container_selection',
          propName: 'engineSize',
          propertyName: 'engineSize',
          tooltip: 'Number or Executor nodes for this engine.',
          label: 'Engine Size (Nodes)',
          selectorType: 'select',
          size: 446,
          options: [
            ...ENGINE_SIZE_STANDARD_OPTIONS,
            {
              value: -1,
              label: 'Custom...',
              container: {
                layout: 'row',
                elements: [
                  {
                    type: 'number',
                    propName: 'containerCount',
                    propertyName: 'containerCount',
                    label: 'Number of Nodes',
                    tooltip: 'Number of execution nodes',
                    size: 126
                  },
                  {
                    type: 'select',
                    propName: 'instanceType',
                    propertyName: 'instanceType',
                    label: 'Engine Node Type',
                    tooltip: 'The instance type to use in for the nodes of this engine. You can choose an instance type that has more CPU, more memory or more NVMe to optimize this engine for your query workload. If you’re not sure, choose the Standard instance type.',
                    size: 310,
                    options: AWS_INSTANCE_TYPE_OPTIONS
                  }
                ]
              }
            }
          ]
        }
      ]
    },
    {
      name: 'Advanced Options',
      collapsible: {initCollapsed: true},
      elements: [
        {
          type: 'checkbox',
          propName: 'useClusterPlacementGroup',
          propertyName: 'useClusterPlacementGroup',
          tooltip: 'Use placement groups to pack instances close together inside an Availability Zone. This provides higher performance but may require more time to start all of the nodes of this engine. (optional)',
          label: 'Use Clustered Placement'
        },
        {
          type: 'checkbox',
          propName: 'disablePublicIp',
          propertyName: 'disablePublicIp',
          tooltip: 'Don\'t associate public IPs with the nodes of this engine.',
          label: 'Disable Public IPs'
        },
        {
          type: 'text',
          propName: 'sshKeyName',
          propertyName: 'sshKeyName',
          size: 446,
          tooltip: ' The name of the EC2 key pair that you will use to SSH into the nodes of this engine if necessary (e.g., to collect log files for troubleshooting).',
          label: 'EC2 Key Pair'
        },
        {
          type: 'text',
          propName: 'securityGroupId',
          propertyName: 'securityGroupId',
          size: 'half',
          tooltip: 'Security group for EC2 instances in this engine, e.g. “sg-0e7662f1d7a81abff”. Leave blank to use the coordinator node’s security group. (optional)',
          label: 'Security Group ID'
        },
        {
          type: 'textarea',
          propName: 'extraConfProps',
          propertyName: 'extraConfProps',
          tooltip: 'Additional Dremio configuration options (optional)',
          label: 'Extra Dremio Configuration Properties'
        },
        {
          type: 'property_list',
          propName: 'awsTags',
          propertyName: 'awsTags',
          addLabel: 'Add tag',
          emptyLabel: '(No tags added)',
          label: 'Engine Tags'
        }
      ]
    }
  ]
};
