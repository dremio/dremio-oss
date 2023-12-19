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
import { intl } from "@app/utils/intl";

const VIEW = "VIEW";
const TABLE = "TABLE";
const FOLDER = "FOLDER";
const VIRTUAL_DATASET = "VIRTUAL_DATASET";
const DATASET = "dataset";
const PHYSICAL_DATASET_HOME_FILE = "PHYSICAL_DATASET_HOME_FILE";
const PHYSICAL_DATASET = "PHYSICAL_DATASET";
const PHYSICAL_DATASET_SOURCE_FILE = "PHYSICAL_DATASET_SOURCE_FILE";
const FILE = "file";
const PHYSICALDATASET = "physicalDataset";
const PHYSICAL_DATASET_SOURCE_FOLDER = "PHYSICAL_DATASET_SOURCE_FOLDER";

const getModalType = (type: string) => {
  switch (type) {
    case VIRTUAL_DATASET:
      return VIEW;
    case DATASET:
      return VIEW;
    case PHYSICAL_DATASET_HOME_FILE:
      return TABLE;
    case PHYSICAL_DATASET:
      return TABLE;
    case PHYSICAL_DATASET_SOURCE_FILE:
      return TABLE;
    case FILE:
      return TABLE;
    case PHYSICALDATASET:
      return TABLE;
    case PHYSICAL_DATASET_SOURCE_FOLDER:
      return FOLDER;
    case FOLDER.toLowerCase():
      return FOLDER;
    default:
      return VIEW;
  }
};

export const getModalTitle = (type: string, entityName: string) => {
  const modalType = getModalType(type);
  switch (modalType) {
    case VIEW:
      return intl.formatMessage({ id: "Modal.Title.View" }, { entityName });
    case TABLE:
      return intl.formatMessage({ id: "Modal.Title.Table" }, { entityName });
    case FOLDER:
      return intl.formatMessage({ id: "Modal.Title.Folder" }, { entityName });
    default:
      return intl.formatMessage({ id: "Modal.Title.Dataset" }, { entityName });
  }
};
