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

import { ENGINE_SIZE, CLUSTER_STATE } from '@app/constants/provisioningPage/provisioningConstants';

export function isYarn(entity) {
  return entity.get('clusterType') === 'YARN';
}

export function isEc2(entity) {
  return entity.get('clusterType') === 'EC2';
}

export function getYarnSubProperty(entity, propName) {
  const subProperty = entity.getIn(['yarnProps', 'subPropertyList'])
    .find((i) => i.get('key') === propName);
  return subProperty && subProperty.get('value');
}

export function getEntityName(entity, propName) {
  return entity.get('name')
    || isYarn(entity) && getYarnSubProperty(entity, propName)
    || entity.get('clusterType');
}

export function getQueuesForEngine(entity, queues) {
  if (!entity || !queues) return queues;
  const engineId = entity.get('name'); //queues currently map to name instead of id
  return queues.filter(queue => queue.get('engineId') === engineId);
}

export function makeContainerPropertyRow(listItem) {
  const containerPropertyList = listItem.get('containerPropertyList');
  //convert array of properties into object with table cell rendering fn
  return containerPropertyList.reduce((prev, property) => {
    return {...prev, [property.get('key')]: {node: () => property.get('value')}};
  }, {});
}

export function makeExecutorInfoRow(listItem) {
  const listEntries = [...listItem].map(([key, value]) => ({ key, value }));

  return listEntries.reduce((prev, property) => {
    return {...prev, [property.key]: {node: () => property.value}};
  }, {});
}
export function getExecutorId(listItem) {
  return listItem.get('instanceId');
}

export function makeDefaultNodePropertyRow(columns, emptySymbol = '-') {
  return columns.reduce((prev, column) => {
    return {...prev, [column]: {node: () => emptySymbol}};
  }, {});
}

export function getContainerListItemId(listItem) {
  if (!listItem) return '-';
  const containerPropertyList = listItem.get('containerPropertyList');
  if (containerPropertyList && containerPropertyList.size) {
    const propertyItem = containerPropertyList.find(item => item.get('key') === 'instanceId');
    return (propertyItem && propertyItem.get('value')) || '';
  } else {
    // in case property list is not available instanceId is containerId
    return listItem.get('containerId');
  }
}

export function getEngineStatusCounts(engine) {
  if (!engine) return { active: 0, pending: 0, disconnected: 0, decommissioning: 0 };

  const workersSummary = engine.get('workersSummary');
  const {
    active, pending, disconnected, decommissioning
  } = workersSummary && workersSummary.toJS() || {};
  return { active, pending, disconnected, decommissioning };
}

export function getEngineSizeLabel(nodeCount) {
  // find size option by nodeCount
  const sizeOption = ENGINE_SIZE.find(size => size.value === nodeCount);
  if (sizeOption) {
    return sizeOption.label;
  }
  // custom size
  return `${ENGINE_SIZE[ENGINE_SIZE.length - 1].label} - ${nodeCount}`;
}

export function getIsInReadOnlyState(engine) {
  if (!engine || !engine.get) return false;
  const currentState = engine.get('currentState');
  return (currentState === CLUSTER_STATE.starting
    || currentState === CLUSTER_STATE.stopping
    || engine.get('desiredState') === CLUSTER_STATE.deleted);
}

export function getNodeCount(engine) {
  const workersSummary = engine.get('workersSummary');
  const { total } = workersSummary && workersSummary.toJS() || {};
  return total || 0;
}
