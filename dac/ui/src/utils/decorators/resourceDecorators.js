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
import Immutable from 'immutable';
import { splitFullPath } from 'utils/pathUtils';
import { ENTITY_TYPES } from '@app/constants/Constants';

export function decorateSource(source) {
  const uiProperties = Immutable.Map({
    isActivePin: false,
    isFile: false,
    entityType: 'source'
  });
  return source.merge(uiProperties);
}

export function decorateSpace(space) {
  const uiProperties = Immutable.Map({
    iconClass: 'Space',
    isFile: false,
    entityType: ENTITY_TYPES.space
  });
  return space.merge(uiProperties);
}

export function decoratePhysicalDataset(dataset, parentPath) {
  const uiProperties = Immutable.fromJS({
    id: dataset.getIn(['datasetConfig', 'id']),
    fileType: 'physicalDatasets',
    entityType: 'physicalDataset',
    parentPath,
    fullPathList: dataset.getIn(['datasetConfig', 'fullPathList']),
    name: dataset.get('datasetName')
  });
  return dataset.merge(uiProperties);
}

export function decorateDataset(dataset, parentPath) {
  const uiProperties = Immutable.Map({
    fileType: 'dataset',
    entityType: 'dataset',
    parentPath,
    fullPathList: dataset.getIn(['datasetConfig', 'fullPathList']),
    name: dataset.get('datasetName')
  });
  return dataset.merge(uiProperties);
}

export function decorateDatasetUI(dataset) {
  let ds = dataset;

  // mike says:
  // Looks like this was added for right tree context, which has since been disabled.
  // Let's leave it for now until we can remove it wholesale.
  // https://github.com/dremio/dremio/commit/05c75f37f097d5cbd745fb2319872acb613d9eff
  const uiProperties = Immutable.Map({
    entityType: 'datasetUI'
  });
  ds = ds.merge(uiProperties);

  return ds;
}

export function decorateFolder(folder) {
  const uiProperties = Immutable.Map({
    fileType: 'folder',
    entityType: 'folder'
  });
  return folder.merge(uiProperties);
}

export function decorateFile(file) {
  // todo: remove hacks: making files "quack" like other things
  // pending new API in https://dremio.atlassian.net/browse/DX-4760
  const uiProperties = Immutable.fromJS({
    fileType: 'file',
    entityType: 'file',
    resourcePath: file.get('id'),
    fullPathList: splitFullPath(file.get('filePath'))
  });
  return file.merge(uiProperties);
}


export function decorateFileFormat(fileFormat) {
  // TODO because of difficulties flattening FileFormatUI on the server, do it here.
  return fileFormat.merge(fileFormat.get('fileFormat')).remove('fileFormat');
}

export function decorateProvision(provision) {
  const sumField = (workerList, fieldName) => workerList.reduce((prevField, nextField) => {
    const containerProperty = (nextField.get('containerPropertyList') || Immutable.List())
      .find(i => i.get('key') === fieldName) || Immutable.Map({value: 0});
    return prevField + parseInt(containerProperty.get('value'), 10);
  }, 0);
  const containers = provision.get('containers') || Immutable.Map();
  const runningWorkers = containers.get('runningList') || Immutable.List();
  const disconnectedWorkers = containers.get('disconnectedList') || Immutable.List();
  const decommissioningCount = containers.get('decommissioningCount') || 0;
  const uiProperties = Immutable.fromJS({
    workersSummary: {
      total: provision.getIn(['dynamicConfig', 'containerCount']) || 0,
      active: runningWorkers.size - decommissioningCount,
      pending: containers.get('pendingCount') || 0,
      disconnected: disconnectedWorkers.size,
      decommissioning: decommissioningCount,
      totalRAM: sumField(runningWorkers, 'memoryMB') || 0,
      totalCores: sumField(runningWorkers, 'virtualCoreCount') || 0
    }
  });
  return provision.merge(uiProperties);
}
