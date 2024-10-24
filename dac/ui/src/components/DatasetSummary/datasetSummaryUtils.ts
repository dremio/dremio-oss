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

import { ENTITY_TYPES } from "#oss/constants/Constants";

const VIRTUAL_DATASET = "VIRTUAL_DATASET";
const PHYSICAL_DATASET = "PHYSICAL_DATASET";
const PHYSICAL_DATASET_HOME_FILE = "PHYSICAL_DATASET_HOME_FILE";
const PHYSICAL_DATASET_HOME_FOLDER = "PHYSICAL_DATASET_HOME_FOLDER";
const PHYSICAL_DATASET_SOURCE_FOLDER = "PHYSICAL_DATASET_SOURCE_FOLDER";
const PHYSICAL_DATASET_SOURCE_FILE = "PHYSICAL_DATASET_SOURCE_FILE";
const ARCTIC_VIEW_INHERITED = "IcebergView";
const ARCTIC_TABLE_INHERITED = "IcebergTable";

export const getIconType = (datasetType: string, isVersioned?: boolean) => {
  let iconName;
  switch (datasetType) {
    case ARCTIC_VIEW_INHERITED:
      iconName = "iceberg-view";
      break;
    case ARCTIC_TABLE_INHERITED:
      iconName = "iceberg-table";
      break;
    case PHYSICAL_DATASET:
      if (isVersioned) {
        iconName = "iceberg-table";
      } else {
        iconName = "dataset-table";
      }
      break;
    case PHYSICAL_DATASET_HOME_FILE:
      iconName = "dataset-table";
      break;
    case PHYSICAL_DATASET_SOURCE_FILE:
      iconName = "dataset-table";
      break;
    case VIRTUAL_DATASET:
      if (isVersioned) {
        iconName = "iceberg-view";
      } else {
        iconName = "dataset-view";
      }
      break;
    case PHYSICAL_DATASET_SOURCE_FOLDER:
      iconName = "purple-folder";
      break;
    case PHYSICAL_DATASET_HOME_FOLDER:
      iconName = "purple-folder";
      break;
    default:
      iconName = undefined;
  }
  return iconName ? iconName : getIconEntityType(datasetType, iconName);
};

const getIconEntityType = (datasetType: string, icon: string | undefined) => {
  let iconName: string | undefined = icon;
  switch (datasetType?.toLowerCase()) {
    case ENTITY_TYPES.folder:
    case "blue-folder":
      iconName = "blue-folder";
      break;
    case ENTITY_TYPES.space:
      iconName = "space";
      break;
    case ENTITY_TYPES.home:
      iconName = "home";
      break;
    default:
      iconName = undefined;
  }
  return iconName;
};

export const addTooltip = (
  ref: { current: any },
  setShowTooltip: (arg: boolean) => void,
) => {
  const current = ref?.current;
  if (current.offsetWidth < current.scrollWidth) {
    setShowTooltip(true);
  }
};

export const openInNewTab = (
  to: string,
  fullPath: string,
  sessionItemName: string,
) => {
  const win = window.open(to, "_blank");
  win && win.sessionStorage.setItem(sessionItemName, fullPath);
};
