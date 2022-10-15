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
const VIRTUAL_DATASET = "VIRTUAL_DATASET";
const PHYSICAL_DATASET = "PHYSICAL_DATASET";
const PHYSICAL_DATASET_HOME_FILE = "PHYSICAL_DATASET_HOME_FILE";
const PHYSICAL_DATASET_HOME_FOLDER = "PHYSICAL_DATASET_HOME_FOLDER";
const PHYSICAL_DATASET_SOURCE_FOLDER = "PHYSICAL_DATASET_SOURCE_FOLDER";
const PHYSICAL_DATASET_SOURCE_FILE = "PHYSICAL_DATASET_SOURCE_FILE";

export const getIconType = (datasetType: string) => {
  let iconName;
  switch (datasetType) {
    case PHYSICAL_DATASET:
      iconName = "dataset-table";
      break;
    case PHYSICAL_DATASET_HOME_FILE:
      iconName = "dataset-table";
      break;
    case PHYSICAL_DATASET_SOURCE_FILE:
      iconName = "dataset-table";
      break;
    case VIRTUAL_DATASET:
      iconName = "dataset-view";
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
  return iconName;
};

export const addTooltip = (
  ref: { current: any },
  setShowTooltip: (arg: boolean) => void
) => {
  const current = ref?.current;
  if (current.offsetWidth < current.scrollWidth) {
    setShowTooltip(true);
  }
};

export const openInNewTab = (
  to: string,
  fullPath: string,
  sessionItemName: string
) => {
  const win = window.open(to, "_blank");
  win && win.sessionStorage.setItem(sessionItemName, fullPath);
};
