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

export const VIRTUAL_DATASET = 'VIRTUAL_DATASET';
export const PHYSICAL_DATASET = 'PHYSICAL_DATASET';
export const PHYSICAL_DATASET_SOURCE_FILE = 'PHYSICAL_DATASET_SOURCE_FILE';
export const PHYSICAL_DATASET_SOURCE_FOLDER = 'PHYSICAL_DATASET_SOURCE_FOLDER';
export const PHYSICAL_DATASET_HOME_FILE = 'PHYSICAL_DATASET_HOME_FILE';
export const PHYSICAL_DATASET_HOME_FOLDER = 'PHYSICAL_DATASET_HOME_FOLDER';

export const DATASET_TYPES_TO_ICON_TYPES = {
  [VIRTUAL_DATASET]: 'VirtualDataset',
  [PHYSICAL_DATASET]: 'PhysicalDataset',
  [PHYSICAL_DATASET_SOURCE_FILE]: 'File',
  [PHYSICAL_DATASET_SOURCE_FOLDER]: 'FolderData',
  [PHYSICAL_DATASET_HOME_FILE]: 'File'
//  [PHYSICAL_DATASET_HOME_FOLDER]: 'FolderData'
};

export const datasetTypeToEntityType = {
  [VIRTUAL_DATASET]: 'dataset',
  [PHYSICAL_DATASET]: 'physicalDataset',
  [PHYSICAL_DATASET_SOURCE_FILE]: 'file',
  [PHYSICAL_DATASET_SOURCE_FOLDER]: 'folder',
  [PHYSICAL_DATASET_HOME_FILE]: 'file'
//  [PHYSICAL_DATASET_HOME_FOLDER]: 'folder'
};

export const PHYSICAL_DATASET_TYPES = new Set([
  PHYSICAL_DATASET,
  PHYSICAL_DATASET_SOURCE_FILE,
  PHYSICAL_DATASET_SOURCE_FOLDER,
  PHYSICAL_DATASET_HOME_FILE,
  PHYSICAL_DATASET_HOME_FOLDER
]);
