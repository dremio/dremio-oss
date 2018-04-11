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
import { DATASET_TYPES_TO_ICON_TYPES } from 'constants/datasetTypes';
import { formatMessage } from './locale';

const FILE_TYPES_TO_ICON_TYPES = {
  database: 'Database',
  table: 'PhysicalDataset',
  dataset: 'VirtualDataset',
  physicalDatasets: 'PhysicalDataset'
};
export function getIconDataTypeFromEntity(entity) {
  const fileType = entity.get('fileType');
  if (fileType === 'folder') {
    if (entity.get('queryable')) {
      return 'FolderData';
    }
    return 'Folder';
  }
  if (fileType === 'file') {
    if (entity.get('queryable')) {
      return 'File';
    }
    return 'FileEmpty';
  }
  return FILE_TYPES_TO_ICON_TYPES[fileType] || 'FileEmpty';
}

export function getIconDataTypeFromDatasetType(datasetType) {
  return DATASET_TYPES_TO_ICON_TYPES[datasetType];
}

const DATABASE_STATUSES_TO_ICON_TYPES = {
  good: 'Database',
  bad: 'Database-Bad',
  degraded: 'Database-Degraded'
};
export function getIconStatusDatabase(status) {
  return DATABASE_STATUSES_TO_ICON_TYPES[status] || 'Database';
}

export function getIconByEntityType(type) {
  switch (type && type.toUpperCase()) {
  case 'DATASET':
  case 'VIRTUAL':
  case 'VIRTUAL_DATASET':
    return 'VirtualDataset';
  case 'PHYSICALDATASET':
  case 'PHYSICAL_DATASET':
  case 'PHYSICAL':
  case 'TABLE':
    return 'PhysicalDataset';
  case 'SPACE':
    return 'Space';
  case 'SOURCE':
    return 'Database';
  case 'HOME':
    return 'Home';
  case 'FILE':
    return 'File';
  case 'PHYSICAL_DATASET_SOURCE_FILE':
    return 'File';
  case 'PHYSICAL_DATASET_SOURCE_FOLDER':
    return 'FolderData';
  case 'PHYSICAL_DATASET_HOME_FILE':
    return 'File';
  case 'FOLDER':
    return 'Folder';
  default:
    return 'FileEmpty';
  }
}

export function getFormatMessageIdByEntityIconType(iconType) {
  switch (iconType) {
  case 'VirtualDataset':
    return 'Dataset.VirtualDataset';
  case 'PhysicalDataset':
    return 'Dataset.PhysicalDataset';
  case 'Space':
    return 'Space.Space';
  case 'Database':
    return 'Source.Source';
  case 'Home':
    return 'Common.Home';
  case 'File':
    return 'File.File';
  case 'FolderData':
    return 'Folder.FolderData';
  case 'Folder':
    return 'Folder.Folder';
  default:
    return 'File.FileEmpty';
  }
}

export function getArtPropsByEntityIconType(iconType) {
  return {
    src: `${iconType}.svg`,
    alt: formatMessage(getFormatMessageIdByEntityIconType(iconType))
  };
}
