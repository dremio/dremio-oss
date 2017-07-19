/*
 * Copyright (C) 2017 Dremio Corporation
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
  bad: 'Connection-Bad',
  degraded: 'Connection-Degraded'
};
export function getIconStatusDatabase(status) {
  return DATABASE_STATUSES_TO_ICON_TYPES[status] || 'Database';
}
